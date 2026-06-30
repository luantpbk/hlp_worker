#!/bin/bash

set -e

echo "=============================="
echo " TikTok HLP Worker Installer PRO "
echo "=============================="

WORKER_NAME="$1"
ENV_CONTENT="$2"
PROXY_COUNT="$3"

# Kiểm tra đảm bảo truyền đủ 4 tham số
if [ -z "$WORKER_NAME" ] || [ -z "$ENV_CONTENT" ] || [ -z "$PROXY_COUNT" ]; then
    echo \"Usage: bash install-debian.sh <worker_name> <env_content> <proxy_count>\"
    exit 1
fi

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# ==========================================
# SAFE APT FIX (CRITICAL)
# ==========================================

echo "=== Fix APT / NodeSource conflicts ==="

rm -f /etc/apt/sources.list.d/nodesource.list
rm -f /etc/apt/sources.list.d/node*.list
rm -f /etc/apt/keyrings/nodesource.gpg
rm -f /usr/share/keyrings/nodesource.gpg

apt clean || true

# ==========================================
# SYSTEM UPDATE (SAFE)
# ==========================================

echo "=== System update ==="

apt update -y || {
    echo "[WARN] apt update failed, retrying..."
    apt --fix-missing update -y || true
}

# ==========================================
# BASE PACKAGES (SKIP IF EXISTS)
# ==========================================

PACKAGES=(curl wget git nano ca-certificates gnupg)

for pkg in "${PACKAGES[@]}"; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
        echo "[INSTALL] $pkg"
        apt install -y "$pkg"
    else
        echo "[OK] $pkg exists"
    fi
done

# ==========================================
# NODEJS INSTALL (SAFE + NO DUPLICATE)
# ==========================================

if command -v node >/dev/null 2>&1; then
    echo "[OK] Node exists: $(node -v)"
else
    echo "=== Install NodeJS 20 ==="

    mkdir -p /etc/apt/keyrings

    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
    | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg

    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" \
    > /etc/apt/sources.list.d/nodesource.list

    apt update -y
    apt install -y nodejs
fi

echo "Node: $(node -v)"
echo "NPM: $(npm -v)"

# ==========================================
# WORKER SYNC (PRO GIT MODE)
# ==========================================

echo "=== Worker sync ==="

if [ -d "/root/hlp_worker" ]; then
    echo "[OK] Worker exists -> updating"

    cd /root/hlp_worker

    if [ -d ".git" ]; then
        git fetch origin || true
        git reset --hard origin/main 2>/dev/null || git reset --hard origin/master
        git clean -fd
    else
        cd /root
        rm -rf hlp_worker
        git clone https://github.com/luantpbk/hlp_worker.git /root/hlp_worker
        cd /root/hlp_worker
    fi
else
    git clone https://github.com/luantpbk/hlp_worker.git /root/hlp_worker
    cd /root/hlp_worker
fi

# ==========================================
# NPM INSTALL (SMART)
# ==========================================
npm install



# ==========================================
# CONFIG UPDATE
# ==========================================

echo "=== Config worker ==="

# Cập nhật Worker Name
sed -i "s/\"workerName\": *\".*\"/\"workerName\": \"$WORKER_NAME\"/g" lc_config.json || true

# Cập nhật số lượng Proxy
sed -i "s/\"proxyCount\": *[0-9]*/\"proxyCount\": $PROXY_COUNT/g" lc_config.json || true


echo "SOCKET_SECRET=\"$ENV_CONTENT\"" > .env

# ==========================================
# PM2 INSTALL CHECK
# ==========================================

if ! command -v pm2 >/dev/null 2>&1; then
    npm install -g pm2
else
    echo "[OK] PM2 exists"
fi

# ==========================================
# START WORKER (CLEAN RESTART)
# ==========================================

echo "=== Restart worker ==="

pm2 delete hlp_worker || true
pm2 start index.js --name hlp_worker 
pm2 save
pm2 startup systemd -u root --hp /root || true

# ==========================================
# DONE
# ==========================================

echo "===================="
echo " INSTALL COMPLETED "
echo "===================="

pm2 list
