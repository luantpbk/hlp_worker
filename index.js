// lc_worker.js
process.env.TZ = "Asia/Ho_Chi_Minh";
require("dotenv").config();
const { io: ClientIO } = require("socket.io-client");
const customParser = require("socket.io-msgpack-parser");

const { HttpsProxyAgent } = require("https-proxy-agent");
const axios = require("axios");
const { gotScraping } = require("got-scraping");
const fs = require("fs");
const WebSocket = require("ws");
const zlib = require("zlib");

const CONFIG_FILE = "lc_config.json";

let config = {
  masterUrl: "http://localhost:3001",
  workerName: `LC_Worker_01`,
  proxyCount: 5,
};
let masterMaxLoadPerProxy = 15;
let currentDynamicMaxLoad = 0;

let globalConnectTokens = config.proxyCount;
let lastRefill = Date.now();
setInterval(() => {
  const now = Date.now();
  if (now - lastRefill >= 2000) {
    globalConnectTokens = config.proxyCount;
    lastRefill = now;
  }
}, 500);

function loadConfig() {
  if (fs.existsSync(CONFIG_FILE)) {
    try {
      config = {
        ...config,
        ...JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8")),
      };
    } catch (e) {}
  } else {
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4));
  }
}
loadConfig();

let watchTimeout = null;
fs.watch(CONFIG_FILE, (eventType) => {
  if (eventType === "change") {
    if (watchTimeout) clearTimeout(watchTimeout);
    watchTimeout = setTimeout(() => {
      try {
        const newConfig = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"));
        let isChanged = false;

        if (
          newConfig.loadPerProxy !== undefined &&
          masterMaxLoadPerProxy !== newConfig.loadPerProxy
        ) {
          masterMaxLoadPerProxy = newConfig.loadPerProxy;
          isChanged = true;
        }

        if (
          newConfig.proxyCount !== undefined &&
          config.proxyCount !== newConfig.proxyCount
        ) {
          config.proxyCount = newConfig.proxyCount;
          isChanged = true;
        }

        if (isChanged && masterSocket?.connected) {
          logWarn(`⚙️ Cập nhật cấu hình nóng. Đang cân bằng lại hệ thống...`);
          checkProxyHealth();
        }
      } catch (err) {}
    }, 2000);
  }
});

let activeConnections = {};
let assignedProxies = {};
let proxyUsage = {};
let proxyFailCount = {};
let proxyCooldown = {};
let proxyStrikeCount = {};
let proxyHealth = {};
let pendingChecks = new Map();
let connectionLocks = new Map();
let masterSocket = null;
let workerPausedUntil = 0;
let hasIPv6Support = false;
let dynamicProxies = [];
let zombieProxies = {};

let proxyLocaleMap = {};
let proxyTimezoneMap = {};

const agentCache = {};
let localTaskQueue = {};

function getGeoParams(countryCode) {
  const geoMap = {
    VN: { lang: "vi-VN", region: "VN" },
    US: { lang: "en-US", region: "US" },
    TH: { lang: "th-TH", region: "TH" },
    ID: { lang: "id-ID", region: "ID" },
    MY: { lang: "ms-MY", region: "MY" },
    PH: { lang: "en-PH", region: "PH" },
    SG: { lang: "en-SG", region: "SG" },
    JP: { lang: "ja-JP", region: "JP" },
    KR: { lang: "ko-KR", region: "KR" },
    TW: { lang: "zh-TW", region: "TW" },
    RU: { lang: "ru-RU", region: "RU" },
    BR: { lang: "pt-BR", region: "BR" },
  };
  return geoMap[countryCode?.toUpperCase()] || { lang: "en-US", region: "US" };
}

function getShortProxy(p) {
  if (!p) return "Unknown";
  if (p === "local") return "Mạng LC (Local)";
  if (typeof p === "string") return p.split("@").pop();
  return String(p);
}

const ENABLE_DEBUG = process.env.DEBUG || process.env.DEBUG === "true";
function logInfo(msg) {
  if (ENABLE_DEBUG) console.log(`[ℹ️] ${msg}`);
}
function logSuccess(msg) {
  console.log(`[✅] ${msg}`);
}
function logWarn(msg) {
  console.warn(`[⚠️] ${msg}`);
}
function logError(msg) {
  console.error(`[❌] ${msg}`);
}

function retireProxy(proxyStr) {
  const isManaged =
    dynamicProxies.includes(proxyStr) || zombieProxies[proxyStr];
  if (!isManaged) return;
  dynamicProxies = dynamicProxies.filter((p) => p !== proxyStr);

  if ((proxyUsage[proxyStr] || 0) > 0) {
    if (!zombieProxies[proxyStr]) {
      logWarn(
        `🧟 Proxy [${getShortProxy(proxyStr)}] bị phế truất nhưng đang gánh ${proxyUsage[proxyStr]} tải. Chuyển sang ZOMBIE.`,
      );
      zombieProxies[proxyStr] = true;
    }
  } else {
    logWarn(
      `🗑️ Proxy [${getShortProxy(proxyStr)}] bị phế truất (Tải = 0). Dọn dẹp RAM ngay!`,
    );
    cleanupProxyData(proxyStr);
    delete zombieProxies[proxyStr];
  }
}

function sendWorkerStatus() {
  if (masterSocket && masterSocket.connected) {
    const activeNames = Object.keys(activeConnections);
    let allPendingTasks = Object.values(localTaskQueue)
      .flat()
      .map((c) => c.username);
    let allConnecting = Array.from(connectionLocks.keys());
    let allPending = [
      ...Array.from(pendingChecks.keys()),
      ...allPendingTasks,
      ...allConnecting,
    ].filter((uname) => !activeNames.includes(uname));
    allPending = [...new Set(allPending)];
    masterSocket.emit("worker_status", {
      currentLoad: activeNames.length + allPending.length,
      runningChannels: activeNames,
      pendingChannels: allPending,
      proxyUsage: proxyUsage,
      socketMap: { ...assignedProxies },
      assignedProxiesList: dynamicProxies,
    });
  }
}
setInterval(sendWorkerStatus, 10000);

setInterval(
  () => {
    if (masterSocket && masterSocket.connected) {
      let heldAssets = [...dynamicProxies];
      if (heldAssets.length > 0)
        masterSocket.emit("worker_heartbeat", heldAssets);
    }
  },
  2 * 60 * 1000,
);

function updateDynamicCapacity() {
  let aliveProxyLoad = 0;
  for (let p of dynamicProxies) {
    if (proxyHealth[p]?.status === "SẴN SÀNG")
      aliveProxyLoad += masterMaxLoadPerProxy || 0;
  }

  if (aliveProxyLoad !== currentDynamicMaxLoad) {
    currentDynamicMaxLoad = aliveProxyLoad;
    if (masterSocket?.connected)
      masterSocket.emit("worker_update_capacity", {
        maxLoad: currentDynamicMaxLoad,
      });
  }
}

async function checkProxyHealth() {
  let checkList = [...dynamicProxies];
  let currentHealth = {};

  const chunkSize = 5;
  for (let i = 0; i < checkList.length; i += chunkSize) {
    const chunk = checkList.slice(i, i + chunkSize);
    await Promise.all(
      chunk.map(async (p) => {
        if (proxyCooldown[p] && Date.now() < proxyCooldown[p]) {
          let remain = Math.ceil((proxyCooldown[p] - Date.now()) / 1000);
          currentHealth[p] = { status: `ĐANG NGHỈ (${remain}s)` };
          return;
        }

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 15000);
        let options = { signal: controller.signal };

        if (p !== "local") {
          const proxyAgent = getCachedAgent(p);
          options.httpAgent = proxyAgent;
          options.httpsAgent = proxyAgent;
        }

        try {
          const healthRes = await axios.get(
            "https://clients3.google.com/generate_204",
            options,
          );
          clearTimeout(timeoutId);

          if (healthRes.status >= 200 && healthRes.status < 600) {
            currentHealth[p] = {
              status: "SẴN SÀNG",
              country: proxyLocaleMap[p] || "en-US",
            };
            proxyFailCount[p] = 0;
          } else throw new Error(`HTTP Lỗi ${healthRes.status}`);
        } catch (e) {
          clearTimeout(timeoutId);
          currentHealth[p] = { status: "MẤT KẾT NỐI" };
          if (p !== "local") {
            if (proxyFailCount[p] < 0) return;
            proxyFailCount[p] = (proxyFailCount[p] || 0) + 1;
            const errMsg = e.message || "";
            if (
              errMsg.includes("402") ||
              errMsg.includes("407") ||
              errMsg.includes("Payment")
            )
              proxyFailCount[p] = 3;
            logWarn(
              `[PING LỖI] Proxy [${getShortProxy(p)}] đứt mạng (Lần ${proxyFailCount[p]}/3). Lỗi: ${errMsg}`,
            );

            if (proxyFailCount[p] >= 3) {
              currentHealth[p].status = "BÁO LỖI";
              if (masterSocket?.connected)
                masterSocket.emit("worker_report_dead_proxy", {
                  proxy: p,
                  workerName: config.workerName,
                });
              retireProxy(p);
              proxyFailCount[p] = -9999;
            } else {
              proxyCooldown[p] = Date.now() + 20000;
            }
          } else {
            proxyCooldown["local"] = Date.now() + 20000;
          }
        }
      }),
    );
  }
  proxyHealth = currentHealth;
  updateDynamicCapacity();
}
setInterval(checkProxyHealth, 5 * 60 * 1000);
setTimeout(checkProxyHealth, 2000);

function getNextAvailableProxy() {
  let allProxies = [...dynamicProxies];
  let available = allProxies.filter((p) => {
    if (proxyCooldown[p] && Date.now() < proxyCooldown[p]) return false;
    if ((proxyStrikeCount[p] || 0) >= 4) return false;
    const isReady = proxyHealth[p]?.status === "SẴN SÀNG";
    const limit = masterMaxLoadPerProxy;
    return isReady && (proxyUsage[p] || 0) < limit;
  });
  if (available.length === 0) return null;
  available.sort((a, b) => (proxyUsage[a] || 0) - (proxyUsage[b] || 0));
  return available[0];
}

function formatProxyUrl(rawProxy) {
  if (!rawProxy || typeof rawProxy !== "string") return null;
  rawProxy = rawProxy.trim();
  if (
    rawProxy.startsWith("http://") ||
    rawProxy.startsWith("https://") ||
    rawProxy.startsWith("socks")
  )
    return rawProxy;
  if (rawProxy.includes("@")) return `http://${rawProxy}`;

  const parts = rawProxy.split(":");
  if (parts.length === 4 && !rawProxy.includes("["))
    return `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
  else if (parts.length === 2 && !rawProxy.includes("["))
    return `http://${parts[0]}:${parts[1]}`;

  if (parts.length >= 6) {
    const pass = parts.pop();
    const user = parts.pop();
    const port = parts.pop();
    const ipv6Raw = parts.join(":");
    const safeIpv6 = ipv6Raw.startsWith("[") ? ipv6Raw : `[${ipv6Raw}]`;
    return `http://${user}:${pass}@${safeIpv6}:${port}`;
  }
  return `http://${rawProxy}`;
}

function getCachedAgent(proxyStr) {
  if (!proxyStr || proxyStr === "local") return undefined;
  const proxyUrl = formatProxyUrl(proxyStr);
  if (!agentCache[proxyUrl]) {
    agentCache[proxyUrl] = new HttpsProxyAgent(proxyUrl, {
      keepAlive: true,
      keepAliveMsecs: 15000, // Giảm xuống 15 giây
      maxSockets: 256, // Tránh thắt cổ chai số lượng kết nối trên mỗi proxy
      maxFreeSockets: 256,
      timeout: 30000, // Đặt timeout cứng ở tầng socket proxy
      rejectUnauthorized: false,
    });
  }
  return agentCache[proxyUrl];
}

function cleanupProxyData(proxy) {
  const proxyUrl = formatProxyUrl(proxy);
  if (proxyUrl && agentCache[proxyUrl]) {
    try {
      agentCache[proxyUrl].destroy();
    } catch (e) {}
    delete agentCache[proxyUrl];
  }
  delete proxyHealth[proxy];
  delete proxyUsage[proxy];
  delete proxyFailCount[proxy];
  delete proxyCooldown[proxy];
  delete proxyStrikeCount[proxy];
}

function safeEmitRadarResult({ channel, status, proxy }) {
  if (masterSocket?.connected)
    masterSocket.emit("radar_result", { channel, status, proxy });
}

function convertProxyForMaster(proxyUrl) {
  if (!proxyUrl || proxyUrl === "local") return proxyUrl;
  try {
    const url = new URL(proxyUrl);
    const user = url.username || "";
    const pass = url.password || "";
    const host = url.hostname;
    const port = url.port;
    if (user && pass) {
      return `${host}:${port}:${user}:${pass}`;
    }
    return `${host}:${port}`;
  } catch (e) {
    return proxyUrl;
  }
}

// ==========================================
// 💡 CẦU NỐI YÊU CẦU CHỮ KÝ TỪ MASTER (PC_SIGN)
// ==========================================
function requestSignFromMaster(username, proxy) {
  return new Promise((resolve, reject) => {
    if (!masterSocket?.connected)
      return reject(new Error("Mất kết nối với Master"));

    const reqId = `SIGN_${Date.now()}_${Math.floor(Math.random() * 1000)}`;

    // Đặt đồng hồ chờ Master và PC_Sign làm việc (Tối đa 25s)
    const timeoutHandle = setTimeout(() => {
      masterSocket.off("worker_receive_sign", listener);
      reject(new Error("Lỗi Timeout: Bot PC_Sign phản hồi quá chậm"));
    }, 300000);

    const listener = (response) => {
      if (response.reqId === reqId) {
        clearTimeout(timeoutHandle);
        masterSocket.off("worker_receive_sign", listener);
        if (response.error) reject(new Error(response.error));
        else {
          response.reqId = reqId; // Lưu lại reqId để báo cáo khi ngắt
          resolve(response);
        }
      }
    };

    masterSocket.on("worker_receive_sign", listener);
    masterSocket.emit("worker_request_sign", {
      reqId,
      username,
      proxy,
      vpsSocketId: masterSocket.id,
    });
  });
}

let disconnectTimer = null;
function connectToMaster() {
  if (masterSocket) masterSocket.disconnect();
  masterSocket = ClientIO(config.masterUrl, {
    auth: {
      token: process.env.SOCKET_SECRET,
      role: "worker",
      clientId: config.workerName,
    },
    reconnection: true,
    reconnectionDelay: 1000,
    transports: ["websocket"],
    parser: customParser,
  });

  masterSocket.on("connect_error", (err) => {
    logError(
      `LỖI KẾT NỐI MASTER: ${err.message}. Hãy kiểm tra IP hoặc Mật khẩu!`,
    );
  });

  masterSocket.on("master_config", (cfg) => {
    if (cfg.MAX_LOAD_PER_PROFILE) {
      masterMaxLoadPerProxy = parseInt(cfg.MAX_LOAD_PER_PROFILE);
      console.log(
        `[ℹ️] Đã nhận cấu hình từ Master: Load/Proxy = ${masterMaxLoadPerProxy}`,
      );
      sendWorkerStatus();
    }
  });

  masterSocket.on("connect", () => {
    logSuccess("Đã kết nối tới Master Hub!");
    if (disconnectTimer) {
      clearTimeout(disconnectTimer);
      disconnectTimer = null;
    }

    masterSocket.emit("worker_ready", {
      name: config.workerName,
      type: "lc_worker",
      maxLoad: currentDynamicMaxLoad,
      loadPerProxy: masterMaxLoadPerProxy,
      proxyCount: config.proxyCount,
      runningChannels: Object.keys(activeConnections),
      pendingChannels: Array.from(pendingChecks.keys()),
      heldProxies: dynamicProxies,
      activeLibrary: "local",
      supportIPv6: hasIPv6Support,
      heldKeys: [],
      heldTiktoolKeys: [],
      socketMap: { ...assignedProxies }, // 💡 Báo cáo map kênh → proxy cho master
    });

    const neededProxies = Math.max(
      0,
      config.proxyCount - dynamicProxies.length,
    );
    if (neededProxies > 0)
      masterSocket.emit("worker_request_proxies", {
        count: neededProxies,
        workerName: config.workerName,
        supportIPv6: hasIPv6Support,
      });

    logSuccess(
      `🔄 Khởi tạo luồng bằng [Mạng Lưới PC_Sign Local]. Đây là LC Worker thuần túy!`,
    );
  });

  masterSocket.on("worker_receive_proxies", (assignedProxiesArr) => {
    assignedProxiesArr.forEach((p) => {
      const proxyStr = typeof p === "string" ? p : p.proxy;
      const locale = typeof p === "string" ? "en-US" : p.locale || "en-US";
      const timezone =
        typeof p === "string"
          ? "America/New_York"
          : p.timezone || "America/New_York"; // <-- CHÈN THÊM
      proxyLocaleMap[proxyStr] = locale;
      proxyTimezoneMap[proxyStr] = timezone;
      if (!dynamicProxies.includes(proxyStr)) {
        dynamicProxies.push(proxyStr);
        proxyStrikeCount[proxyStr] = 0;
        proxyCooldown[proxyStr] = 0;
        if (!proxyHealth) proxyHealth = {};
        proxyHealth[proxyStr] = { status: "SẴN SÀNG" };
      }
    });
    logSuccess(`📥 Đã nhận ${assignedProxiesArr.length} Proxy từ Master.`);
    checkProxyHealth();
  });

  masterSocket.on("worker_proxy_replacement", (data) => {
    const { deadProxy, newProxy } = data;
    retireProxy(deadProxy);
    if (newProxy) {
      const pStr = typeof newProxy === "string" ? newProxy : newProxy.proxy;
      const locale =
        typeof newProxy === "string" ? "en-US" : newProxy.locale || "en-US";
      proxyLocaleMap[pStr] = locale;
      if (zombieProxies[pStr]) delete zombieProxies[pStr];
      if (!dynamicProxies.includes(pStr)) dynamicProxies.push(pStr);
      proxyStrikeCount[pStr] = 0;
      proxyCooldown[pStr] = 0;
      if (!proxyHealth) proxyHealth = {};
      proxyHealth[pStr] = { status: "SẴN SÀNG" };
      logSuccess(
        `🔄 Đổi máu: Phế truất [${getShortProxy(deadProxy)}] -> Nạp mới [${getShortProxy(pStr)}]`,
      );
    }
    checkProxyHealth();
  });

  masterSocket.on("worker_proxy_removed", (proxyStr) => {
    retireProxy(proxyStr);
    logWarn(`🗑️ Lệnh từ Admin: Thu hồi Proxy [${getShortProxy(proxyStr)}]`);
    checkProxyHealth();
  });

  masterSocket.on("cmd_pause_worker", () => {
    logWarn(
      "⏸️ Nhận lệnh TẠM DỪNG từ Master. Ngừng nhận kênh mới, chờ xả tải dần...",
    );
    workerPausedUntil = Infinity;
  });
  masterSocket.on("cmd_resume_worker", () => {
    logSuccess("▶️ Nhận lệnh TIẾP TỤC từ Master. Bắt đầu nhận kênh mới!");
    workerPausedUntil = 0;
  });
  masterSocket.on("cmd_stop_worker", () => {
    logWarn(
      "⏹️ Nhận lệnh DỪNG HẲN (STOP) từ Master. Rút điện toàn bộ hệ thống!",
    );
    workerPausedUntil = Infinity;
    localTaskQueue = {};
    connectionQueue = {};
    const usersToClean = new Set([
      ...Object.keys(activeConnections),
      ...connectionLocks.keys(),
      ...Object.keys(assignedProxies),
    ]);
    usersToClean.forEach((user) => {
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
      stopWebcast(user);
    });
    connectionLocks.clear();
  });

  masterSocket.on("process_task", (taskData) => {
    if (!taskData || !taskData.username) return;

    // 💡 Tách checkProxy ra khỏi channel object
    const checkProxy = taskData.checkProxy || null;
    const channel = { ...taskData };
    delete channel.checkProxy;

    // 💡 Guard: chỉ nhận task khi đã có proxy (từ master) hoặc có proxy pool riêng
    if (!checkProxy && dynamicProxies.length === 0) {
      return safeEmitRadarResult({ channel, status: "REQUEUE" });
    }

    const proxyKey = checkProxy;
    if (!proxyKey)
      return safeEmitRadarResult({ channel, status: "SYSTEM_BUSY" });
    if (!localTaskQueue[proxyKey]) localTaskQueue[proxyKey] = [];

    const maxAllowedQueue = Math.max(20, currentDynamicMaxLoad * 2);
    let totalLocalTasks = Object.values(localTaskQueue).reduce(
      (sum, arr) => sum + arr.length,
      0,
    );

    if (totalLocalTasks >= maxAllowedQueue)
      return safeEmitRadarResult({ channel, status: "REQUEUE" });

    const isInLocalQueue = Object.values(localTaskQueue).some((arr) =>
      arr.some((c) => c.username === channel.username),
    );
    const isInConnectionQueue = Object.values(connectionQueue).some((arr) =>
      arr.some((item) => item.channel.username === channel.username),
    );

    if (
      !isInLocalQueue &&
      !pendingChecks.has(channel.username) &&
      !activeConnections[channel.username] &&
      !connectionLocks.has(channel.username) &&
      !isInConnectionQueue
    ) {
      // 💡 Lưu kèm proxy được master chỉ định
      localTaskQueue[proxyKey].push({ ...channel, _masterProxy: checkProxy });
    }
  });

  masterSocket.on("force_update_config", (newCfg) => {
    if (newCfg.proxyCount !== undefined)
      config.proxyCount = parseInt(newCfg.proxyCount);
    if (newCfg.loadPerProxy !== undefined)
      masterMaxLoadPerProxy = parseInt(newCfg.loadPerProxy);

    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4));
    sendWorkerStatus();
    checkProxyHealth();
    balanceResources();
  });

  masterSocket.on("disconnect", (reason) => {
    if (reason === "io server disconnect") masterSocket.connect();
    else {
      disconnectTimer = setTimeout(() => {
        localTaskQueue = {};
        connectionQueue = {};
        const usersToClean = new Set([
          ...Object.keys(activeConnections),
          ...connectionLocks.keys(),
          ...Object.keys(assignedProxies),
        ]);
        usersToClean.forEach((user) => stopWebcast(user));
        activeConnections = {};
        assignedProxies = {};
        pendingChecks.clear();
        connectionLocks.clear();
        disconnectTimer = null;
      }, 600000);
    }
  });

  masterSocket.on("cmd_stop_all", () => {
    for (let username in activeConnections) stopWebcast(username);
  });
}

let isProcessingQueue = {};
setInterval(async () => {
  if (Date.now() < workerPausedUntil) return;

  const allProxies = [
    ...new Set([
      ...dynamicProxies,
      ...Object.keys(localTaskQueue),
      ...Object.keys(connectionQueue),
    ]),
  ].filter((p) => p && p !== "local");
  for (const proxyKey of allProxies) {
    if (!localTaskQueue[proxyKey] || localTaskQueue[proxyKey].length === 0)
      continue;
    if (isProcessingQueue[proxyKey]) continue;

    // Giới hạn số lượng checkLive và pending trong connectionQueue phù hợp với mỗi luồng proxy (<= 3)
    const currentConnectionLen = connectionQueue[proxyKey]
      ? connectionQueue[proxyKey].length
      : 0;
    if (currentConnectionLen >= 3) continue;

    isProcessingQueue[proxyKey] = true;
    try {
      const taskItem = localTaskQueue[proxyKey].shift();
      const { _masterProxy, ...channel } = taskItem;
      pendingChecks.set(channel.username, Date.now());

      // Delay random xíu để tản tải HTTP request
      setTimeout(
        () => {
          executeTask(channel, _masterProxy);
        },
        Math.floor(Math.random() * 1000),
      );
    } finally {
      isProcessingQueue[proxyKey] = false;
    }
  }
}, 1000);

async function checkLiveStatus(username, proxy) {
  let proxyUrlGot = proxy === "local" ? undefined : formatProxyUrl(proxy);
  const urlUsername = username.startsWith("@") ? username : `@${username}`;
  let retries = 1;
  while (retries >= 0) {
    try {
      const proxyLocale = proxyLocaleMap[proxy] || "en-US";
      const proxyTz = proxyTimezoneMap[proxy] || "America/New_York"; // <-- Kéo múi giờ ra
      const langCode = proxyLocale.split("-")[0];

      const fetchPromise = gotScraping({
        url: `https://www.tiktok.com/${urlUsername}/live?lang=${langCode}&tz_name=${encodeURIComponent(proxyTz)}`,
        headers: {
          Cookie: `timezone_name=${proxyTz};`,
        },
        proxyUrl: proxyUrlGot,
        timeout: { request: 12000 },
        throwHttpErrors: false,
        http2: true,
        retry: { limit: 0 },
        headerGeneratorOptions: {
          browsers: [{ name: "chrome", minVersion: 120 }],
          devices: ["desktop"],
          locales: [proxyLocale, "en-US"],
        },
      });

      let timeoutHandle;
      const hardTimeout = new Promise((_, r) => {
        timeoutHandle = setTimeout(() => {
          if (typeof fetchPromise.cancel === "function") {
            fetchPromise.cancel();
          }
          r(new Error("HARD_TIMEOUT"));
        }, 12000);
      });

      let res;
      try {
        fetchPromise.catch(() => {});
        res = await Promise.race([fetchPromise, hardTimeout]);
      } finally {
        clearTimeout(timeoutHandle);
      }

      if ([403, 429].includes(res.statusCode)) {
        logWarn(
          `[TIKTOK BLOCK] HTTP ${res.statusCode} chặn kết nối - Proxy: ${getShortProxy(proxy)}`,
        );
        return "RATE_LIMIT";
      }
      if (
        [407, 502, 503, 504].includes(res.statusCode) ||
        res.statusCode >= 500
      ) {
        logWarn(
          `[PROXY ERROR] HTTP ${res.statusCode} Proxy chết yếu - Proxy: ${getShortProxy(proxy)}`,
        );
        return "PROXY_ERR";
      }
      if (res.statusCode === 404) return "NOT_FOUND";

      const finalUrl = (res.url || "").toLowerCase();
      if (
        finalUrl.includes("login") ||
        finalUrl.includes("verify") ||
        finalUrl.includes("captcha")
      ) {
        logWarn(
          `[TIKTOK CAPTCHA] Bị ép giải Captcha URL - Proxy: ${getShortProxy(proxy)}`,
        );
        return "CAPTCHA";
      }

      const html =
        typeof res.body === "string"
          ? res.body
          : JSON.stringify(res.body || "");

      if (
        html.includes("Just a moment...") ||
        html.includes("Challenge Validation") ||
        html.includes("cf-browser-verification")
      ) {
        logWarn(
          `[CLOUDFLARE WAF] Bị chặn ngầm (Bot Detect) - Proxy: ${getShortProxy(proxy)}`,
        );
        return "RATE_LIMIT";
      }

      if (
        html.includes('"statusCode":10000') ||
        html.includes("webapp.not-found")
      )
        return "NOT_FOUND";

      const isLiveFlag =
        html.includes('"status":2') || html.includes('"isLive":true');
      const roomMatch = html.match(/"(?:roomId|room_id)"\s*:\s*"?([1-9]\d+)"?/);
      if (roomMatch && roomMatch[1] && isLiveFlag)
        return "LIVE|" + roomMatch[1];

      return "OFFLINE";
    } catch (error) {
      retries--;
      if (retries < 0) {
        if (error.message === "HARD_TIMEOUT") {
          logWarn(
            `[HARD TIMEOUT] Treo kết nối quá 15s - Proxy: ${getShortProxy(proxy)}`,
          );
          return "NETWORK_ERR";
        }
        logWarn(
          `[NETWORK/TIMEOUT] Lỗi ngầm - Proxy: ${getShortProxy(proxy)} | Lỗi: ${error.message}`,
        );
        if (
          error.message.includes("402") ||
          error.message.includes("407") ||
          error.message.includes("Payment")
        ) {
          return "FATAL_PROXY_BILLING";
        }
        return "NETWORK_ERR";
      }
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

// ==========================================
// 💡 HÀNG ĐỢI KẾT NỐI & GIỚI HẠN TỐC ĐỘ (RATE LIMIT LOCAL SIGN)
// ==========================================
let connectionQueue = {};
let isConnecting = {};
let pendingSigns = {}; // Đếm số lượng đang xin chữ ký Python

setInterval(() => {
  if (Date.now() < workerPausedUntil) return;
  const allProxies = [
    ...new Set([
      ...dynamicProxies,
      ...Object.keys(localTaskQueue),
      ...Object.keys(connectionQueue),
    ]),
  ].filter((p) => p && p !== "local");
  for (const proxyKey of allProxies) {
    if (
      isConnecting[proxyKey] ||
      !connectionQueue[proxyKey] ||
      connectionQueue[proxyKey].length === 0
    )
      continue;
    if ((pendingSigns[proxyKey] || 0) >= 1) continue; // 💡 YÊU CẦU CỦA USER: Chạy LẦN LƯỢT từng kênh một trên mỗi proxy (không nhồi nhiều tab)

    isConnecting[proxyKey] = true;
    try {
      const item = connectionQueue[proxyKey][0];
      const { channel, proxy, roomId: fetchedRoomId } = item;

      if (
        !connectionLocks.has(channel.username) ||
        activeConnections[channel.username]
      ) {
        connectionQueue[proxyKey].shift();
        if (!connectionLocks.has(channel.username))
          stopWebcast(channel.username);
        isConnecting[proxyKey] = false;
        continue;
      }

      if (globalConnectTokens <= 0) {
        isConnecting[proxyKey] = false;
        continue;
      }

      connectionQueue[proxyKey].shift();
      globalConnectTokens--;
      pendingSigns[proxyKey] = (pendingSigns[proxyKey] || 0) + 1;

      // 💡 Chạy ngầm việc cắm Socket, xong việc thì giảm biến đếm
      startWebcast(channel, proxy, fetchedRoomId).finally(() => {
        pendingSigns[proxyKey]--;
      });

      // 💡 GỠ BỎ NÚT THẮT CỔ CHAI: Chỉ cần đợi 3s - 4s cho kênh tiếp theo
      const totalDelay = 3000 + Math.floor(Math.random() * 1000);
      setTimeout(() => {
        isConnecting[proxyKey] = false;
      }, totalDelay);
    } catch (err) {
      isConnecting[proxyKey] = false;
    }
  }
}, 100);

async function executeTask(channel, masterProxy) {
  if (
    activeConnections[channel.username] ||
    connectionLocks.has(channel.username)
  ) {
    pendingChecks.delete(channel.username);
    return;
  }

  let checkProxy = masterProxy;

  if (!checkProxy) {
    // 💡 Fallback: Tự chọn proxy nếu master không cung cấp (backward compat)
    let availableProxies = [];

    for (let p of dynamicProxies) {
      if (
        proxyHealth[p]?.status === "SẴN SÀNG" &&
        (!proxyCooldown[p] || Date.now() > proxyCooldown[p]) &&
        (proxyStrikeCount[p] || 0) < 4
      )
        availableProxies.push(p);
    }

    if (availableProxies.length === 0) {
      pendingChecks.delete(channel.username);
      return safeEmitRadarResult({ channel, status: "SYSTEM_BUSY" });
    }
    checkProxy =
      availableProxies[Math.floor(Math.random() * availableProxies.length)];
  }

  try {
    let rawStatus = await checkLiveStatus(channel.username, checkProxy);
    let status = rawStatus;
    let fetchedRoomId = null;
    // 💡 BẢN VÁ: Tách "LIVE" và "roomId" ra
    if (typeof rawStatus === "string" && rawStatus.startsWith("LIVE|")) {
      status = "LIVE";
      fetchedRoomId = rawStatus.split("|")[1];
    }

    if (!pendingChecks.has(channel.username)) {
      logWarn(`[GHOST HTTP] Kênh ${channel.username} check quá lâu. Hủy!`);
      return;
    }
    if (status === "LIVE") {
      logInfo(`${channel.username} LIVE. Thực hiện cắm Socket`);
    }
    if (status === "NOT_FOUND" || status === "OFFLINE") {
      safeEmitRadarResult({ channel, status: status, proxy: checkProxy });
      return;
    }

    if (
      status === "CAPTCHA" ||
      status === "RATE_LIMIT" ||
      status === "PROXY_ERR" ||
      status === "NETWORK_ERR" ||
      status === "FATAL_PROXY_BILLING"
    ) {
      if (checkProxy !== "local") {
        if (
          !dynamicProxies.includes(checkProxy) &&
          !zombieProxies[checkProxy]
        ) {
          safeEmitRadarResult({ channel, status: "REQUEUE" });
          return;
        }
        if (status === "FATAL_PROXY_BILLING") {
          proxyStrikeCount[checkProxy] = 4;
        } else {
          proxyStrikeCount[checkProxy] =
            (proxyStrikeCount[checkProxy] || 0) + 1;
        }
        if (proxyStrikeCount[checkProxy] >= 4) {
          if (masterSocket?.connected)
            masterSocket.emit("worker_report_dead_proxy", {
              proxy: checkProxy,
            });
          retireProxy(checkProxy);
        } else if (proxyStrikeCount[checkProxy] < 4) {
          proxyCooldown[checkProxy] = Date.now() + 60000;
        }
      } else {
        proxyCooldown["local"] = Date.now() + 45000;
      }
      safeEmitRadarResult({ channel, status: "ERROR" });
      return;
    }

    if (status === "LIVE") {
      safeEmitRadarResult({ channel, status: "LIVE" });
      connectionLocks.set(channel.username, Date.now());
      const proxyKey = checkProxy;
      if (!connectionQueue[proxyKey]) connectionQueue[proxyKey] = [];
      connectionQueue[proxyKey].push({
        channel,
        proxy: checkProxy, // Truyền đúng proxy Master thay vì hardcoded "master"
        roomId: fetchedRoomId,
      });
    }
  } catch (e) {
    if (checkProxy !== "local") proxyCooldown[checkProxy] = Date.now() + 30000;
    else proxyCooldown["local"] = Date.now() + 30000;
    setTimeout(() => {
      safeEmitRadarResult({ channel, status: "REQUEUE" });
    }, 3000);
  } finally {
    pendingChecks.delete(channel.username);
  }
}
const { HeaderGenerator } = require("header-generator");
const headerGenerator = new HeaderGenerator({
  browsers: ["chrome", "firefox", "safari"],
  operatingSystems: ["windows", "macos"],
});
const fetchRoomId = async (username, proxyAgent) => {
  const proxyLocale = proxyLocaleMap[proxy] || "en-US";
  const proxyTz = proxyTimezoneMap[proxy] || "America/New_York"; // <-- Kéo múi giờ ra
  const langCode = proxyLocale.split("-")[0];
  const opts = {
    url: `https://www.tiktok.com/@${username}/live?lang=${langCode}&tz_name=${encodeURIComponent(proxyTz)}`,
    headers: {
      Cookie: `timezone_name=${proxyTz};`,
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept-Language": "en-US,en;q=0.9",
    },
    timeout: 10000,
  };
  if (proxyAgent) opts.httpsAgent = proxyAgent;
  const res = await axios(opts);
  const dataStr =
    typeof res.data === "string" ? res.data : JSON.stringify(res.data || "");
  const roomMatch = dataStr.match(/"(?:roomId|room_id)"\s*:\s*"?([1-9]\d+)"?/);
  if (roomMatch) return roomMatch[1];
  throw new Error("Cannot find roomId");
};

let proto = null;

async function startWebcast(channel, proxy, fetchedRoomId) {
  try {
    if (!proto) proto = await import("tiktok-live-proto/v3");
  } catch (err) {
    logError("Không thể load tiktok-live-proto: " + err.message);
    return;
  }
  if (activeConnections[channel.username]) return;

  logInfo(`Bắt đầu WSS (Thuần WebSocket) socket ${channel.username} qua Proxy`);
  try {
    const cleanName = channel.username.startsWith("@")
      ? channel.username.slice(1)
      : channel.username;

    let initialEnvData;
    let cookieStr = "";
    let wsUrl = "";
    let masterProxy = "";
    let cursor = "";
    let internalExt = "";
    const generatedHeaders = headerGenerator.getHeaders();
    let channelUA =
      generatedHeaders["user-agent"] ||
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/120.0.0.0";

    try {
      logInfo(`[WSS] Đang xin Cookie và EnvData từ Master cho ${cleanName}...`);
      initialEnvData = await requestSignFromMaster(
        cleanName,
        convertProxyForMaster(proxy),
      );
      if (initialEnvData) {
        let envDataPayload = initialEnvData.envData || {};
        if (envDataPayload.cursor) cursor = envDataPayload.cursor;
        if (envDataPayload.internal_ext)
          internalExt = envDataPayload.internal_ext;
        if (envDataPayload.userAgent)
          channelUA = envDataPayload.userAgent.replace(/\r?\n|\r/g, "").trim();
        if (envDataPayload.cookies)
          cookieStr = envDataPayload.cookies.replace(/\r?\n|\r/g, "").trim();
        if (envDataPayload.proxy)
          masterProxy = envDataPayload.proxy; // Bắt buộc lấy Proxy của Master
        else masterProxy = proxy; // BẢN VÁ: lấy tham số truyền vào nếu PC_Sign không trả về proxy
        if (envDataPayload.ws_url) {
          wsUrl = envDataPayload.ws_url.trim();
          wsUrl = wsUrl.replace(/^https/i, "wss").replace(/^http/i, "ws");
          logSuccess(
            `[WSS] Đã lấy được ws_url và Cookies từ Master cho ${cleanName}`,
          );
        } else {
          logError(
            `[WSS] Master trả về EnvData nhưng KHÔNG có ws_url cho ${cleanName} (Có thể do lỗi Captcha trên pc_sign)`,
          );
        }
      } else {
        throw new Error("Empty EnvData");
      }
    } catch (e) {
      logError(`Master không trả về dữ liệu kết nối (${e.message})`);
      safeEmitRadarResult({ channel, status: "ERROR" });
      stopWebcast(channel.username);
      return;
    }

    if (!wsUrl || !cookieStr) {
      logError("Không thể kết nối WSS: Thiếu ws_url hoặc cookie từ Master.");
      safeEmitRadarResult({ channel, status: "ERROR" });
      stopWebcast(channel.username);
      return;
    }

    let proxyAgent;
    let activeProxy = masterProxy;

    if (activeProxy) {
      let proxyStr = activeProxy;
      if (!proxyStr.startsWith("http")) proxyStr = `http://${activeProxy}`;
      const parts = activeProxy.replace("http://", "").split(":");
      if (parts.length === 4) {
        proxyStr = `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
      }
      proxyAgent = new HttpsProxyAgent(proxyStr);
      if (!agentCache[proxyStr]) agentCache[proxyStr] = proxyAgent;
      proxyAgent = agentCache[proxyStr];

      proxyUsage[activeProxy] = (proxyUsage[activeProxy] || 0) + 1;
      assignedProxies[channel.username] = activeProxy;
    }

    if (!fetchedRoomId) {
      try {
        logInfo(`[WSS] Đang lấy Room ID cho ${cleanName}...`);
        fetchedRoomId = await fetchRoomId(cleanName, proxyAgent);
        logSuccess(`[WSS] Room ID của ${cleanName} là ${fetchedRoomId}`);
      } catch (e) {
        logError(`[WSS] Không lấy được Room ID cho ${cleanName}`);
        safeEmitRadarResult({ channel, status: "ERROR" });
        stopWebcast(channel.username);
        return;
      }
    }

    let currentViewers = 0;
    const deviceId = String(
      Math.floor(Math.random() * 9000000000000000000) + 1000000000000000000,
    );

    const catchTreasureBox = (
      data,
      channel,
      msgName,
      roomId,
      currentViewers,
    ) => {
      const boxData = data?.envelopeInfo || data?.treasureBoxData || data;
      const coins =
        boxData?.diamondCount || boxData?.coin || boxData?.coins || 0;
      const boxes =
        boxData?.peopleCount || boxData?.totalUser || boxData?.boxes || 0;
      let boxType = "tui";
      const bType = boxData?.businessType;
      if (bType === 1 || String(bType) === "1") boxType = "ruong";
      else if (bType === 4 || String(bType) === "4") boxType = "ruong_vang";
      if (coins <= 15) return;
      if (activeConnections[channel.username])
        activeConnections[channel.username].lastActive = Date.now();
      let originTimeMs = Date.now();
      if (data?.common?.createTime) {
        originTimeMs = Number(data.common.createTime);
        if (originTimeMs < 10000000000) originTimeMs *= 1000;
      } else if (data?.timestamp) {
        originTimeMs = Number(data.timestamp);
      }
      if (masterSocket?.connected) {
        masterSocket.emit("worker_chest_raw", {
          channel,
          coins,
          boxes,
          idc:
            boxData?.envelopeId ||
            boxData?.id ||
            boxData?.treasureId ||
            boxData?.envelopeIdc ||
            "",
          workerName: config.workerName,
          liveRegion: channel.country || "unknown",
          unpackAt: boxData?.unpackAt || boxData?.openTime,
          viewers: currentViewers,
          roomId,
          workerTime: originTimeMs,
          isHanging: false,
          type: boxType,
        });
      }
    };

    activeConnections[channel.username] = {
      isConnected: true,
      lastActive: Date.now(),
      roomId: fetchedRoomId,
      seqId: 1,
      reqId: initialEnvData.reqId, // Lưu reqId để báo dập kết nối
    };

    const tlsOptions = {
      ciphers:
        "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-RSA-AES128-SHA:ECDHE-RSA-AES256-SHA:AES128-GCM-SHA256:AES256-GCM-SHA384:AES128-SHA:AES256-SHA:DES-CBC3-SHA",
      sigalgs:
        "ecdsa_secp256r1_sha256:rsa_pss_rsae_sha256:rsa_pkcs1_sha256:ecdsa_secp384r1_sha384:rsa_pss_rsae_sha384:rsa_pkcs1_sha384:rsa_pss_rsae_sha512:rsa_pkcs1_sha512",
      ecdhCurve: "X25519:P-256:P-384:P-521",
      secureProtocol: "TLSv1_2_method",
    };

    const ws = new WebSocket(wsUrl, {
      agent: proxyAgent,
      ...tlsOptions,
      headers: {
        Pragma: "no-cache",
        "Cache-Control": "no-cache",
        "User-Agent": channelUA,
        Cookie: cookieStr,
        Origin: "https://www.tiktok.com",
        Referer: `https://www.tiktok.com/@${cleanName}/live`,
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Sec-WebSocket-Extensions":
          "permessage-deflate; client_max_window_bits",
      },
    });

    activeConnections[channel.username].ws = ws;

    ws.on("open", () => {
      logSuccess(`[WSS] ${channel.username} KẾT NỐI WEBSOCKET THÀNH CÔNG!`);
      // 💡 Báo cáo socket mở cho Master để tracking số kênh thực
      if (masterSocket?.connected) {
        masterSocket.emit("worker_socket_opened", {
          username: channel.username,
          proxy: activeProxy || "local",
        });
      }
      try {
        let imEnterRoomMsg = null;
        if (proto.WebcastImEnterRoomMessage) {
          let uniqueIdStr = "";
          for (let i = 0; i < 18; i++)
            uniqueIdStr += Math.floor(Math.random() * 10);
          if (uniqueIdStr.startsWith("0"))
            uniqueIdStr = "1" + uniqueIdStr.slice(1);

          imEnterRoomMsg = proto.WebcastImEnterRoomMessage.encode({
            roomId: fetchedRoomId || "0",
            scene: "1",
            enterSource: "37",
            accountType: "0",
            filterWelcomeMsg: "0",
            isAnchorContinueKeepMsg: false,
            liveId: "12",
            enterUniqueId: uniqueIdStr,
            roomTag: "",
            liveRegion: "",
            identity: "audience",
            cursor: "",
          }).finish();
        } else {
          imEnterRoomMsg = new Uint8Array();
        }

        const enterFrame = proto.WebcastPushFrame.encode({
          seqId: "0",
          logId: "0",
          payloadEncoding: "pb",
          payloadType: "im_enter_room",
          payload: imEnterRoomMsg,
          service: "0",
          method: "0",
          headers: [],
        }).finish();
        ws.send(enterFrame);
      } catch (e) {
        logError(
          `[WSS] Lỗi khi khởi tạo luồng data cho ${channel.username}: ${e.message}`,
        );
      }
    });

    ws.on("message", (data) => {
      try {
        const frame = proto.WebcastPushFrame.decode(new Uint8Array(data));
        if (frame.payloadType === "msg") {
          let payloadBuffer = frame.payload;
          const isGzip =
            frame.headers &&
            frame.headers.some(
              (h) => h.key === "compress_type" && h.value === "gzip",
            );
          if (isGzip) {
            payloadBuffer = zlib.unzipSync(payloadBuffer);
          }
          const fetchResult =
            proto.ProtoMessageFetchResult.decode(payloadBuffer);

          if (fetchResult.needAck && frame.logId) {
            try {
              const internalExt = fetchResult.internalExt || "";
              const ackBuffer = proto.WebcastPushFrame.encode({
                logId: frame.logId || "0",
                seqId: "0",
                payloadEncoding: "pb",
                payloadType: "ack",
                payload: Buffer.from(internalExt, "utf8"),
                service: "0",
                method: "0",
                headers: [],
              }).finish();
              if (ws.readyState === WebSocket.OPEN) {
                ws.send(ackBuffer);
              }
            } catch (ackErr) {
              logWarn(`[WSS] Lỗi gửi ACK: ${ackErr.message}`);
            }
          }

          if (fetchResult && fetchResult.messages) {
            for (let msg of fetchResult.messages) {
              if (
                msg.method === "WebcastEnvelopeMessage" ||
                msg.method === "WebcastTreasureBoxMessage"
              ) {
                try {
                  let payloadObj = null;
                  const protoMethod =
                    msg.method === "WebcastTreasureBoxMessage"
                      ? "WebcastEnvelopeMessage"
                      : msg.method;
                  if (proto[protoMethod]) {
                    payloadObj = proto[protoMethod].decode(msg.payload);
                  }
                  if (payloadObj) {
                    const msgName =
                      msg.method === "WebcastTreasureBoxMessage"
                        ? "treasureBox"
                        : "envelope";
                    catchTreasureBox(
                      payloadObj,
                      channel,
                      msgName,
                      fetchedRoomId,
                      currentViewers,
                    );
                  }
                } catch (decErr) {
                  logWarn(`[⚠️] Lỗi decode ${msg.method}: ${decErr.message}`);
                }
              }
            }
          }
        }
      } catch (err) {
        logWarn(
          `[WSS] Lỗi xử lý message WebcastPushFrame từ ${channel.username}: ${err.message}`,
        );
      }
    });

    ws.on("unexpected-response", (request, response) => {
      const msg =
        response.headers["handshake-msg"] ||
        `Unexpected ${response.statusCode}`;
      logError(`[WSS] @${channel.username} Bị từ chối kết nối: ${msg}`);

      request.abort();
      stopWebcast(channel.username);

      // Nếu proxy lỗi thì tăng biến đếm
      if (activeProxy && activeProxy !== "local") {
        proxyFailCount[activeProxy] = (proxyFailCount[activeProxy] || 0) + 1;
        if (proxyFailCount[activeProxy] >= 5 && proxyHealth[activeProxy]) {
          proxyHealth[activeProxy].status = "LỖI KẾT NỐI LIÊN TỤC";
          proxyCooldown[activeProxy] = Date.now() + 180000;
        }
      }
      setTimeout(
        () => safeEmitRadarResult({ channel, status: "REQUEUE" }),
        3000,
      );
    });

    ws.on("error", (err) => {
      logError(`[WSS] ${channel.username} Lỗi: ${err.message}`);
      stopWebcast(channel.username);
      setTimeout(
        () => safeEmitRadarResult({ channel, status: "REQUEUE" }),
        3000,
      );
    });

    ws.on("close", (code, reason) => {
      const reasonStr = reason ? reason.toString() : "No reason provided";
      logWarn(
        `[WSS] ${channel.username} Đóng kết nối (Code: ${code}, Reason: ${reasonStr})`,
      );

      stopWebcast(channel.username);

      // Phân loại lỗi để có chiến lược phục hồi
      if (code === 1006) {
        logInfo(
          `[WSS] Đang thử khôi phục kết nối nhanh cho ${channel.username}...`,
        );
        // Gọi lại startWebcast hoặc đẩy vào một hàng đợi ưu tiên cao (Fast-lane queue)
        // Tùy chỉnh trạng thái để master không tính là một kênh chết hoàn toàn
        setTimeout(
          () => {
            safeEmitRadarResult({ channel, status: "FAST_RECONNECT" });
          },
          1500 + Math.random() * 2000,
        ); // Thêm jitter ngẫu nhiên để tránh thundering herd
      } else {
        setTimeout(
          () => safeEmitRadarResult({ channel, status: "REQUEUE" }),
          3000,
        );
      }
    });
  } catch (e) {
    logError(`Lỗi kết nối cho ${channel.username}: ${e.message}`);
    let p = masterProxy || proxy;
    if (p && p !== "local") {
      proxyFailCount[p] = (proxyFailCount[p] || 0) + 1;
      if (proxyFailCount[p] >= 5) {
        if (proxyHealth[p]) proxyHealth[p].status = "LỖI KẾT NỐI LIÊN TỤC";
        proxyCooldown[p] = Date.now() + 180000;
      }
    }
    safeEmitRadarResult({ channel, status: "ERROR" });
    stopWebcast(channel.username);
  }
}

function stopWebcast(user) {
  const conn = activeConnections[user];
  if (conn && conn.reqId && masterSocket && masterSocket.connected) {
    masterSocket.emit("worker_report_disconnect", { reqId: conn.reqId });
  }

  const realProxy = assignedProxies[user];

  // 💡 Báo cáo socket đóng cho Master để cập nhật proxySocketCount
  if (masterSocket?.connected) {
    masterSocket.emit("worker_socket_closed", {
      username: user,
      proxy: realProxy,
    });
  }

  if (realProxy) {
    proxyUsage[realProxy] = Math.max(0, (proxyUsage[realProxy] || 0) - 1);
    delete assignedProxies[user];
    if (zombieProxies[realProxy] && proxyUsage[realProxy] === 0) {
      logInfo(
        `👻 Proxy Zombie [${getShortProxy(realProxy)}] đã xả xong. Dọn dẹp RAM!`,
      );
      cleanupProxyData(realProxy);
      delete zombieProxies[realProxy];
    }
  }

  pendingChecks.delete(user);
  connectionLocks.delete(user);

  if (!conn) return;
  delete activeConnections[user];

  setImmediate(() => {
    try {
      if (typeof conn.removeAllListeners === "function")
        conn.removeAllListeners();
      if (typeof conn.disconnect === "function") conn.disconnect();
      if (conn.client && typeof conn.client.removeAllListeners === "function")
        conn.client.removeAllListeners();
      if (conn.ws && typeof conn.ws.terminate === "function")
        conn.ws.terminate();
    } catch (e) {}
  });
}

// Global WSS Ping Loop
setInterval(() => {
  if (!proto || !proto.HeartBeatMessage) return;
  for (let username in activeConnections) {
    const conn = activeConnections[username];
    if (conn && conn.ws && conn.ws.readyState === 1) {
      // 1 = OPEN
      try {
        if (conn.ws.ping) conn.ws.ping(); // Gửi Ping protocol TCP để giữ connection với Proxy/Cloudflare (Fix lỗi 1006)
        let hbMsg = proto.HeartBeatMessage.encode({
          roomId: conn.roomId || "0",
          sendPacketSeqId: String(conn.seqId || 1),
        }).finish();

        const hbFrame = proto.WebcastPushFrame.encode({
          seqId: "0",
          logId: "0",
          payloadEncoding: "pb",
          payloadType: "hb",
          payload: hbMsg,
          service: "0",
          method: "0",
          headers: [],
        }).finish();

        conn.ws.send(hbFrame);
        if (typeof conn.seqId === "number") conn.seqId++;
      } catch (e) {
        // Ignore ping errors
      }
    }
  }
}, 10000);

setInterval(() => {
  const now = Date.now();
  for (let [user, timestamp] of pendingChecks.entries()) {
    if (now - timestamp > 45000) {
      pendingChecks.delete(user);
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
    }
  }
  for (let [user, timestamp] of connectionLocks.entries()) {
    if (!activeConnections[user] && now - timestamp > 240000) {
      logWarn(`[LOCK TIMEOUT] Giải phóng kênh kẹt ${user}. Thu hồi Proxy!`);
      stopWebcast(user);
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
    }
  }

  const currentTotalLoad =
    Object.keys(activeConnections).length +
    connectionQueue.length +
    pendingChecks.size;
  const safeLoad = masterMaxLoadPerProxy > 0 ? masterMaxLoadPerProxy : 15;
  const realMaxLoad = config.proxyCount * safeLoad;
  const isMaxLoad = currentTotalLoad >= realMaxLoad * 0.9;

  for (let user in activeConnections) {
    const conn = activeConnections[user];
    if (now - (conn.lastActive || now) > 90 * 60 * 1000) {
      if (isMaxLoad) {
        logWarn(`✂️ Cắt bỏ Socket Zombie [${user}] sau 90 phút.`);
        stopWebcast(user);
        safeEmitRadarResult({
          channel: { username: user },
          status: "COOLDOWN",
          proxy: assignedProxies[user],
        });
      } else {
        conn.lastActive = now - 80 * 60 * 1000;
      }
    }
  }

  for (let user in assignedProxies) {
    if (!activeConnections[user] && !connectionLocks.has(user)) {
      logWarn(`[MEM LEAK] Kênh ${user} bốc hơi nhưng chiếm Proxy. Thu hồi!`);
      stopWebcast(user);
    }
  }

  const realUsage = {};
  for (let p of dynamicProxies) realUsage[p] = 0;
  for (let user in assignedProxies) {
    const p = assignedProxies[user];
    realUsage[p] = (realUsage[p] || 0) + 1;
  }
  proxyUsage = realUsage;
}, 30000);

function balanceResources() {
  if (masterSocket && masterSocket.connected) {
    const targetProxyCount = parseInt(config.proxyCount, 10) || 0;
    const currentProxyCount = dynamicProxies.length;

    if (currentProxyCount < targetProxyCount) {
      const needed = targetProxyCount - currentProxyCount;
      logWarn(`🔄 Kho Proxy đang thiếu ${needed} cái. Xin Master cấp bù...`);
      masterSocket.emit("worker_request_proxies", {
        count: needed,
        workerName: config.workerName,
        supportIPv6: hasIPv6Support,
      });
    } else if (currentProxyCount > targetProxyCount) {
      const excessCount = currentProxyCount - targetProxyCount;
      const excessProxies = [...dynamicProxies].slice(-Math.abs(excessCount));
      logWarn(`🗑️ Thừa ${excessCount} Proxy. Đang trả lại Master...`);
      masterSocket.emit("worker_return_proxies", excessProxies);
      excessProxies.forEach((p) => retireProxy(p));
      checkProxyHealth();
    }
  }
}
setInterval(balanceResources, 20000);

async function checkIPv6Capability() {
  try {
    logInfo("⏳ Đang kiểm tra kết nối IPv6 của VPS...");
    const res = await axios.get("https://api6.ipify.org", { timeout: 4000 });
    if (res.data) {
      hasIPv6Support = true;
      logSuccess(
        `🌐 VPS CÓ HỖ TRỢ IPV6 (IP: ${res.data}). Sẵn sàng gánh Proxy IPv6!`,
      );
    }
  } catch (e) {
    hasIPv6Support = false;
    logWarn(`🌐 VPS KHÔNG CÓ IPV6. Sẽ yêu cầu Master chỉ cấp Proxy IPv4.`);
  }
}

checkIPv6Capability().then(() => {
  connectToMaster();
});

function handleShutdown(signal) {
  logWarn(`⚠️ Nhận lệnh ${signal}. Đang trả tài nguyên...`);
  if (masterSocket && masterSocket.connected) {
    let proxiesToReturn = [...dynamicProxies];
    if (proxiesToReturn.length > 0)
      masterSocket.emit("worker_return_proxies", proxiesToReturn);
    masterSocket.emit("worker_intentional_disconnect");
  }
  setTimeout(() => {
    process.exit(0);
  }, 1000);
}

process.on("uncaughtException", (err) => {
  logError(`[CRASH PROTECT]: ${err.message}`);
});
process.on("unhandledRejection", (reason) => {
  logError(`[CRASH PROTECT]: ${reason}`);
});
process.on("SIGTERM", handleShutdown);
process.on("SIGINT", handleShutdown);
