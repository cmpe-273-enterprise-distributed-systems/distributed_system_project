#!/usr/bin/env bash
# Bootstrap nginx on EC2 to serve the React production build from /var/www/ai-gateway-client.
#
# Prereqs on the instance:
#   1. Security group: inbound TCP 80 (and 443 if you add TLS later) from your clients.
#   2. Build the app with production API env vars (CRA bakes these at build time):
#        export REACT_APP_UPSTASH_REDIS_REST_URL="https://....upstash.io"
#        export REACT_APP_UPSTASH_REDIS_REST_READ_ONLY_TOKEN="..."
#        export REACT_APP_LEADER_FALLBACK_URL="http://YOUR_LEADER_PUBLIC_IP:8000"
#        npm ci && npm run build
#   3. Copy the build folder to the instance, e.g.:
#        rsync -avz --delete client/build/ ec2-user@YOUR_HOST:/tmp/ai-gateway-build/
#   Then on EC2 (this script):
#        sudo bash deploy/ec2-bootstrap.sh /tmp/ai-gateway-build YOUR_PUBLIC_DNS_OR_IP
#
# Usage: sudo ./deploy/ec2-bootstrap.sh <path-to-build-folder> <server_name>
# Example: sudo ./deploy/ec2-bootstrap.sh ./build ec2-12-34-56-78.compute.amazonaws.com

set -euo pipefail

BUILD_SRC="${1:-}"
SERVER_NAME="${2:-}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run as root: sudo $0 <build-dir> <server_name>" >&2
  exit 1
fi

if [[ -z "${BUILD_SRC}" || -z "${SERVER_NAME}" ]]; then
  echo "Usage: sudo $0 <path-to-build-folder> <server_name>" >&2
  echo "  server_name: EC2 public DNS, public IP, or domain you will use in the browser" >&2
  exit 1
fi

if [[ ! -d "${BUILD_SRC}" ]] || [[ ! -f "${BUILD_SRC}/index.html" ]]; then
  echo "Build folder missing or invalid (expected index.html): ${BUILD_SRC}" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NGINX_TEMPLATE="${SCRIPT_DIR}/nginx/ai-gateway-client.conf"
WEB_ROOT="/var/www/ai-gateway-client"

if [[ ! -f "${NGINX_TEMPLATE}" ]]; then
  echo "Missing template: ${NGINX_TEMPLATE}" >&2
  exit 1
fi

# --- Install nginx ---
if command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y nginx
  NGINX_SITE="/etc/nginx/sites-available/ai-gateway-client.conf"
  NGINX_ENABLED="/etc/nginx/sites-enabled/ai-gateway-client.conf"
  mkdir -p /etc/nginx/sites-available /etc/nginx/sites-enabled
  sed "s/YOUR_DOMAIN_OR_IP/${SERVER_NAME//\//\\/}/g" "${NGINX_TEMPLATE}" > "${NGINX_SITE}"
  ln -sf "${NGINX_SITE}" "${NGINX_ENABLED}"
  # Remove default site if present so our server_name wins for port 80
  rm -f /etc/nginx/sites-enabled/default
elif command -v dnf >/dev/null 2>&1; then
  dnf install -y nginx
  rm -f /etc/nginx/conf.d/default.conf
  NGINX_CONF="/etc/nginx/conf.d/ai-gateway-client.conf"
  sed "s/YOUR_DOMAIN_OR_IP/${SERVER_NAME//\//\\/}/g" "${NGINX_TEMPLATE}" > "${NGINX_CONF}"
else
  echo "No supported package manager (apt-get or dnf). Install nginx manually." >&2
  exit 1
fi

mkdir -p "${WEB_ROOT}"
find "${WEB_ROOT}" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
cp -a "${BUILD_SRC}/." "${WEB_ROOT}/"
chown -R www-data:www-data "${WEB_ROOT}" 2>/dev/null || chown -R nginx:nginx "${WEB_ROOT}" 2>/dev/null || true

nginx -t
systemctl enable nginx
systemctl restart nginx

echo "Done. Open http://${SERVER_NAME}/ in a browser (ensure security group allows TCP 80)."
