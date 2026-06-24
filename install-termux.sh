#!/data/data/com.termux/files/usr/bin/bash

set -e

echo "=============================="
echo " TikTok HLP Worker Termux Install "
echo "=============================="

WORKER_NAME="$1"
ENV_CONTENT="$2"
PROXY_COUNT="$3"
LOCAL_LOAD="$4"

if [ -z "$WORKER_NAME" ] || [ -z "$ENV_CONTENT" ] || [ -z "$PROXY_COUNT" ] || [ -z "$LOCAL_LOAD" ]; then
    echo "Usage:"
    echo "bash install-termux.sh <worker_name> <socket_secret> <proxy_count> <local_load>"
    exit 1
fi

# ==========================================
# TERMUX UPDATE
# ==========================================

echo "=== Update packages ==="

pkg update -y
pkg upgrade -y

# ==========================================
# INSTALL BASE PACKAGES
# ==========================================

echo "=== Install packages ==="

PACKAGES=(git curl wget nano nodejs)

for pkg in "${PACKAGES[@]}"; do
    if ! command -v "${pkg%%-*}" >/dev/null 2>&1; then
        echo "[INSTALL] $pkg"
        pkg install -y "$pkg"
    else
        echo "[OK] $pkg exists"
    fi
done

# ==========================================
# CHECK NODE
# ==========================================

echo "Node: $(node -v)"
echo "NPM: $(npm -v)"

# ==========================================
# WORKER DIRECTORY
# ==========================================

WORKER_DIR="$HOME/hlp_worker"

# ==========================================
# WORKER SYNC
# ==========================================

echo "=== Worker sync ==="

if [ -d "$WORKER_DIR" ]; then
    echo "[OK] Worker exists -> updating"

    cd "$WORKER_DIR"

    if [ -d ".git" ]; then
        git fetch origin || true
        git reset --hard origin/main 2>/dev/null || git reset --hard origin/master
        git clean -fd
    else
        cd "$HOME"
        rm -rf hlp_worker
        git clone https://github.com/luantpbk/hlp_worker.git "$WORKER_DIR"
        cd "$WORKER_DIR"
    fi
else
    git clone https://github.com/luantpbk/hlp_worker.git "$WORKER_DIR"
    cd "$WORKER_DIR"
fi

# ==========================================
# NPM INSTALL
# ==========================================

echo "=== Install npm packages ==="


npm install


# ==========================================
# CONFIG UPDATE
# ==========================================

echo "=== Config worker ==="

# Cập nhật Worker Name
sed -i "s/\"workerName\": *\".*\"/\"workerName\": \"$WORKER_NAME\"/g" lc_config.json || true

# Cập nhật số lượng Proxy
sed -i "s/\"proxyCount\": *[0-9]*/\"proxyCount\": $PROXY_COUNT/g" lc_config.json || true

# Cập nhật số lượng Local Load
sed -i "s/\"localLoad\": *[0-9]*/\"localLoad\": $LOCAL_LOAD/g" lc_config.json || true

echo "SOCKET_SECRET=\"$ENV_CONTENT\"" > .env

# ==========================================
# PM2 INSTALL
# ==========================================

echo "=== Check PM2 ==="

if ! command -v pm2 >/dev/null 2>&1; then
    npm install -g pm2
else
    echo "[OK] PM2 exists"
fi

# ==========================================
# START WORKER
# ==========================================

echo "=== Restart worker ==="

pm2 delete hlp_worker || true

pm2 start index.js --name hlp_worker

pm2 save

# ==========================================
# AUTO START TERMUX
# ==========================================

mkdir -p ~/.termux

cat > ~/.termux/boot.sh <<EOF
#!/data/data/com.termux/files/usr/bin/bash
pm2 resurrect
EOF

chmod +x ~/.termux/boot.sh

# ==========================================
# DONE
# ==========================================

echo ""
echo "===================="
echo " INSTALL COMPLETED "
echo "===================="

pm2 list
