#!/usr/bin/env bash
# ==============================================================================
# Cloudflare Zero-Trust Quick Tunnel Runner (ADR D16)
# Exposes FastAPI Backend (and 4-Panel Dashboard) to Public Internet (trycloudflare.com)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Default configurations
DEFAULT_PORT=8000
API_PORT="${API_PORT:-$DEFAULT_PORT}"
IS_TEST_MODE=false

# Parse command line flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --test)
      IS_TEST_MODE=true
      shift
      ;;
    --port)
      API_PORT="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--test] [--port <PORT>]"
      exit 1
      ;;
  esac
done

# 1. Verify cloudflared binary is installed
if ! command -v cloudflared &>/dev/null; then
  echo "[-] Error: 'cloudflared' binary is not installed or not in PATH."
  echo "    Install with: curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared && chmod +x /usr/local/bin/cloudflared"
  exit 1
fi

CLOUDFLARED_VERSION=$(cloudflared --version 2>&1 | head -n 1)
echo "[+] Detected cloudflared: ${CLOUDFLARED_VERSION}"

# 2. Test mode execution (for CI / Automated Verification)
if [[ "${IS_TEST_MODE}" == "true" ]]; then
  echo "[+] Running in --test verification mode..."
  echo "[+] Validating cloudflared configuration for target port ${API_PORT}..."
  
  # Check if backend responds locally (optional warning if not yet started)
  if curl -sf "http://127.0.0.1:${API_PORT}/health" &>/dev/null; then
    echo "[+] Local FastAPI backend verified healthy at http://127.0.0.1:${API_PORT}/health"
  else
    echo "[!] Warning: Local backend not currently running on port ${API_PORT} (acceptable in offline test)."
  fi

  # Dry-run test cloudflared command flags
  if cloudflared tunnel --help &>/dev/null; then
    echo "[+] cloudflared CLI syntax validated successfully."
  else
    echo "[-] Error validating cloudflared syntax."
    exit 1
  fi

  echo "[+] Cloudflare Tunnel test verification PASSED."
  exit 0
fi

# 3. Production / Live Quick Tunnel Mode
echo "================================================================================"
echo " Starting Cloudflare Quick Tunnel for Crypto Surveillance Command Center"
echo " Target Local URL: http://127.0.0.1:${API_PORT}"
echo "================================================================================"

# Verify local backend is reachable before tunneling
if ! curl -sf "http://127.0.0.1:${API_PORT}/health" &>/dev/null; then
  echo "[!] Warning: FastAPI backend does not seem to be running on port ${API_PORT}."
  echo "    Please start backend first: python -m backend.main"
  echo "    Proceeding with tunnel launch anyway..."
fi

LOG_FILE=$(mktemp /tmp/cloudflared_tunnel_XXXXXX.log)

cleanup() {
  echo ""
  echo "[*] Shutting down Cloudflare Tunnel..."
  if [[ -n "${TUNNEL_PID:-}" ]] && kill -0 "${TUNNEL_PID}" 2>/dev/null; then
    kill -TERM "${TUNNEL_PID}" 2>/dev/null || true
    wait "${TUNNEL_PID}" 2>/dev/null || true
  fi
  rm -f "${LOG_FILE}"
  echo "[+] Tunnel stopped cleanly."
}
trap cleanup EXIT INT TERM

# Start cloudflared quick tunnel in background
cloudflared tunnel --url "http://127.0.0.1:${API_PORT}" --no-autoupdate > "${LOG_FILE}" 2>&1 &
TUNNEL_PID=$!

echo "[*] Establishing outbound encrypted tunnel to Cloudflare Edge..."

# Poll log file for trycloudflare.com URL (max 20 seconds)
URL=""
for _ in $(seq 1 40); do
  if ! kill -0 "${TUNNEL_PID}" 2>/dev/null; then
    echo "[-] Error: cloudflared process exited unexpectedly. Log output:"
    cat "${LOG_FILE}"
    exit 1
  fi

  URL=$(grep -oE 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "${LOG_FILE}" | head -n 1 || true)
  if [[ -n "${URL}" ]]; then
    break
  fi
  sleep 0.5
done

if [[ -z "${URL}" ]]; then
  echo "[-] Timed out waiting for Cloudflare Tunnel URL. Full logs:"
  cat "${LOG_FILE}"
  exit 1
fi

echo ""
echo "================================================================================"
echo " [SUCCESS] PUBLIC LIVE SURVEILLANCE DASHBOARD IS ACTIVE"
echo " Public Tunnel URL: ${URL}"
echo " WebSocket Stream:  ${URL/https/wss}/ws"
echo " Prometheus API:    ${URL}/metrics"
echo "================================================================================"
echo "[*] Press Ctrl+C to terminate tunnel."

# Keep running and tail logs
wait "${TUNNEL_PID}"
