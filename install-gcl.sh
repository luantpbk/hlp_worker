#!/bin/bash

set -e

echo "=============================="
echo " TikTok HLP Worker GCloud Install "
echo "=============================="

WORKER_NAME="$1"
ENV_CONTENT="$2"
PROXY_COUNT="$3"

if [ -z "$WORKER_NAME" ] || [ -z "$ENV_CONTENT" ] || [ -z "$PROXY_COUNT" ] || [ -z "$LOCAL_LOAD" ]; then
    echo "Usage:"
    echo "bash install-gcl.sh <worker_name> <socket_secret> <proxy_count> <local_load>"
    exit 1
fi

# ==========================================
# GCLOUD SHELL SAFE MODE
# ==========================================

if [ "$EUID" -ne 0 ]; then
    SUDO="sudo"
else
    SUDO=""
fi

WORKER_DIR="$HOME/hlp_worker"

# ==========================================
# FIX APT / NODE CONFLICT
# ==========================================

echo "=== Fix APT conflicts ==="

$SUDO rm -f /etc/apt/sources.list.d/nodesource.list || true
$SUDO rm -f /etc/apt/sources.list.d/node*.list || true
$SUDO rm -f /etc/apt/keyrings/nodesource.gpg || true
$SUDO rm -f /usr/share/keyrings/nodesource.gpg || true

$SUDO apt clean || true

# ==========================================
# UPDATE SYSTEM
# ==========================================

echo "=== Update system ==="

$SUDO apt update -y || {
    echo "[WARN] apt update failed"
}

# ==========================================
# INSTALL BASE PACKAGES
# ==========================================

echo "=== Install packages ==="

PACKAGES=(curl wget git nano ca-certificates gnupg)

for pkg in "${PACKAGES[@]}"; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
        echo "[INSTALL] $pkg"
        $SUDO apt install -y "$pkg"
    else
        echo "[OK] $pkg exists"
    fi
done

# ==========================================
# NODEJS INSTALL
# ==========================================

if command -v node >/dev/null 2>&1; then
    echo "[OK] Node exists: $(node -v)"
else
    echo "=== Install NodeJS 20 ==="

    $SUDO mkdir -p /etc/apt/keyrings

    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
    | gpg --dearmor | $SUDO tee /etc/apt/keyrings/nodesource.gpg >/dev/null

    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" \
    | $SUDO tee /etc/apt/sources.list.d/nodesource.list >/dev/null

    $SUDO apt update -y
    $SUDO apt install -y nodejs
fi

echo "Node: $(node -v)"
echo "NPM: $(npm -v)"

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
# DONE
# ==========================================

echo ""
echo "===================="
echo " INSTALL COMPLETED "
echo "===================="

pm2 list
