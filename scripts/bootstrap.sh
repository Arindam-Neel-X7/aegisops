#!/usr/bin/env bash
set -euo pipefail

# 6. REPOSITORY ROOT RESOLUTION
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

log_msg() { echo -e "[AegisOps Bootstrap] $1"; }
log_ok() { echo -e "[AegisOps Bootstrap] [OK] $1"; }
log_err() { echo -e "[AegisOps Bootstrap] [ERROR] $1" >&2; }
die() { log_err "$1"; exit 1; }

log_msg "Stage 1/21: Checking required repository paths..."
REQUIRED_PATHS=(
    "backend/pyproject.toml"
    "backend/poetry.lock"
    "backend/alembic.ini"
    "frontend/package.json"
    "frontend/package-lock.json"
    "docker-compose.yml"
    "backend/app"
    "backend/tests"
    "frontend"
    "scripts"
)
for p in "${REQUIRED_PATHS[@]}"; do
    if [[ ! -e "${ROOT_DIR}/${p}" ]]; then
        die "Missing required path: ${p}"
    fi
done
log_ok "All required paths present."

log_msg "Stage 2/21: Checking prerequisite commands..."
PREREQS=("python" "poetry" "node" "npm" "docker")
for cmd in "${PREREQS[@]}"; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        die "Missing required command: $cmd"
    fi
done
log_ok "All prerequisite commands present."

log_msg "Stage 3/21: Validating Bash version..."
if [[ "${BASH_VERSINFO[0]:-0}" -lt 3 ]] || ( [[ "${BASH_VERSINFO[0]:-0}" -eq 3 ]] && [[ "${BASH_VERSINFO[1]:-0}" -lt 2 ]] ); then
    die "Bash >= 3.2 is required. Detected ${BASH_VERSION:-unknown}."
fi
log_ok "Bash version ok."

log_msg "Stage 4/21: Validating Python version..."
PY_VER=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [[ "$PY_VER" != "3.11" ]]; then
    die "Python 3.11.x is required. Detected $PY_VER."
fi
log_ok "Python version ok."

log_msg "Stage 5/21: Validating Poetry version..."
POETRY_VER=$(poetry --version | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
if [[ "$POETRY_VER" != "2.4.3" ]]; then
    die "Poetry 2.4.3 is required. Detected $POETRY_VER."
fi
log_ok "Poetry version ok."

log_msg "Stage 6/21: Validating Node version..."
NODE_VER=$(node --version)
if [[ "$NODE_VER" != v24.* ]]; then
    die "Node 24.x is required for CI/bootstrap reproducibility. Detected $NODE_VER."
fi
log_ok "Node version ok."
NPM_VER=$(npm --version)
log_msg "Detected npm version: $NPM_VER"

log_msg "Stage 7/21: Validating Docker & Compose..."
if ! docker info >/dev/null 2>&1; then
    die "Docker daemon is unreachable. Is Docker running?"
fi
if ! docker compose version >/dev/null 2>&1; then
    die "'docker compose' v2 command is required."
fi
log_ok "Docker & Compose ok."

log_msg "Stage 8/21: Validating Compose Config..."
if ! docker compose --profile core config --quiet; then
    die "Docker Compose configuration is invalid."
fi
log_ok "Compose config valid."

log_msg "Stage 9/21: Checking port 8000 safety..."
if ! python -c 'import socket; s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.bind(("127.0.0.1", 8000)); s.close()' 2>/dev/null; then
    die "Port 8000 is already occupied. Bootstrap refuses to validate an unknown existing server."
fi
log_ok "Port 8000 is free."

export DATABASE_URL="postgresql+asyncpg://aegisops:aegisops@localhost:5432/aegisops"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
log_msg "Using local bootstrap database on localhost:5432"

log_msg "Stage 10/21: Installing Backend Dependencies..."
cd "${ROOT_DIR}/backend"
poetry install --no-interaction --no-ansi
cd "${ROOT_DIR}"
log_ok "Backend dependencies installed."

log_msg "Stage 11/21: Installing Frontend Dependencies..."
cd "${ROOT_DIR}/frontend"
npm ci
cd "${ROOT_DIR}"
log_ok "Frontend dependencies installed."

log_msg "Stage 12/21: Starting Core Docker Services..."
if ! docker compose --profile core up -d; then
    docker compose --profile core ps
    die "Failed to start core docker services."
fi
log_ok "Core Docker services started."

log_msg "Stage 13/21: Polling Docker Health (postgres, redis)..."
check_docker_health() {
    local svc="$1"
    local attempts=30
    local interval=2
    local count=0
    
    while [ $count -lt $attempts ]; do
        local cid
        cid=$(docker compose --profile core ps -q "$svc")
        if [[ -z "$cid" ]]; then
            die "Container for $svc missing. (no container ID)"
        fi
        
        local state health
        state=$(docker inspect --format='{{.State.Status}}' "$cid" 2>/dev/null || echo "INSPECT_FAIL")
        if [[ "$state" == "INSPECT_FAIL" ]]; then
            die "Failed to inspect container $svc ($cid)."
        elif [[ "$state" == "exited" ]]; then
            die "Container for $svc exited prematurely."
        fi
        
        health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || echo "INSPECT_FAIL")
        if [[ "$health" == "INSPECT_FAIL" ]]; then
            die "Failed to inspect health of $svc ($cid)."
        elif [[ "$health" == "healthy" ]]; then
            log_ok "$svc is healthy."
            return 0
        elif [[ "$health" == "unhealthy" ]]; then
            die "$svc became unhealthy."
        fi
        
        sleep "$interval"
        count=$((count+1))
    done
    docker compose --profile core ps
    docker compose --profile core logs --no-color "$svc"
    die "Timeout waiting for $svc to become healthy."
}

check_docker_health postgres
check_docker_health redis

log_msg "Stage 14/21: Postgres Functional Check..."
if ! docker compose --profile core exec -T postgres pg_isready -U aegisops -d aegisops; then
    die "pg_isready check failed."
fi
log_ok "Postgres functional check passed."

log_msg "Stage 15/21: Redis Functional Check..."
REDIS_PONG=$(docker compose --profile core exec -T redis redis-cli ping | tr -d '\r\n')
if [[ "$REDIS_PONG" != "PONG" ]]; then
    docker compose --profile core exec -T redis redis-cli ping
    die "Redis ping failed. Expected PONG, got: $REDIS_PONG"
fi
log_ok "Redis functional check passed."

log_msg "Stage 16/21: Running Migrations..."
cd "${ROOT_DIR}/backend"
if ! poetry run alembic upgrade head; then
    die "Database migration failed."
fi
cd "${ROOT_DIR}"
log_ok "Migrations applied."

log_msg "Stage 17/21: Starting Temporary Backend..."
UVICORN_LOG=$(mktemp "${TMPDIR:-/tmp}/aegisops_uvicorn_XXXXXX.log")
UVICORN_PID=""

cleanup() {
    local exit_code=$?
    if [[ -n "${UVICORN_PID:-}" ]] && kill -0 "$UVICORN_PID" 2>/dev/null; then
        kill "$UVICORN_PID" 2>/dev/null || true
        wait "$UVICORN_PID" 2>/dev/null || true
    fi
    if [[ $exit_code -ne 0 ]]; then
        if [[ -n "${UVICORN_LOG:-}" ]] && [[ -f "$UVICORN_LOG" ]]; then
            log_err "Uvicorn logs:"
            cat "$UVICORN_LOG" >&2
            rm -f "$UVICORN_LOG"
        fi
        log_msg "Hint: Docker services were left running for debugging. Run 'docker compose --profile core down' to clean up."
    else
        if [[ -n "${UVICORN_LOG:-}" ]] && [[ -f "$UVICORN_LOG" ]]; then
            rm -f "$UVICORN_LOG"
        fi
    fi
    exit $exit_code
}
trap cleanup EXIT

cd "${ROOT_DIR}/backend"
POETRY_ENV_PATH=$(poetry env info --path)
PYTHON_EXE="${POETRY_ENV_PATH}/bin/python"
if [[ ! -x "$PYTHON_EXE" ]]; then
    die "Virtualenv python executable not found at: $PYTHON_EXE"
fi
"$PYTHON_EXE" -m uvicorn app.main:app --host 127.0.0.1 --port 8000 > "$UVICORN_LOG" 2>&1 &
UVICORN_PID=$!
cd "${ROOT_DIR}"

sleep 2
if ! kill -0 "$UVICORN_PID" 2>/dev/null; then
    die "Uvicorn exited early."
fi
log_ok "Temporary backend started (PID $UVICORN_PID)."

log_msg "Stage 18/21: Polling /health Endpoint..."
check_http() {
    local url="$1"
    local expect_status="$2"
    local expect_version="$3"
    
    local script
    read -r -d '' script << 'EOF' || true
import sys, urllib.request, json
try:
    req = urllib.request.urlopen(sys.argv[1], timeout=2)
    if req.getcode() != 200: sys.exit(1)
    body = json.loads(req.read().decode('utf-8'))
    if body.get('status') != sys.argv[2]: sys.exit(2)
    if sys.argv[3] and body.get('version') != sys.argv[3]: sys.exit(3)
    sys.exit(0)
except Exception:
    sys.exit(4)
EOF

    local count=0
    while [ $count -lt 15 ]; do
        if ! kill -0 "$UVICORN_PID" 2>/dev/null; then
            die "Uvicorn exited prematurely during polling."
        fi
        
        if python -c "$script" "$url" "$expect_status" "$expect_version"; then
            return 0
        fi
        sleep 2
        count=$((count+1))
    done
    return 1
}

if ! check_http "http://127.0.0.1:8000/health" "ok" "0.1.0"; then
    die "/health endpoint failed semantic check or timed out."
fi
log_ok "/health endpoint ok."

log_msg "Stage 19/21: Polling /ready Endpoint..."
if ! check_http "http://127.0.0.1:8000/ready" "ready" ""; then
    die "/ready endpoint failed semantic check or timed out."
fi
log_ok "/ready endpoint ok."

log_msg "Stage 20/21: Terminating Temporary Backend..."
if kill -0 "$UVICORN_PID" 2>/dev/null; then
    kill "$UVICORN_PID"
    wait "$UVICORN_PID" 2>/dev/null || true
fi
UVICORN_PID=""
log_ok "Temporary backend terminated."

log_msg "Stage 21/21: Running Quality Gates..."
cd "${ROOT_DIR}/backend"
log_msg "Backend: ruff check..."
poetry run ruff check app tests || die "ruff check failed"
log_msg "Backend: mypy..."
poetry run mypy app || die "mypy failed"
log_msg "Backend: pytest..."
poetry run pytest tests -q || die "pytest failed"

cd "${ROOT_DIR}/frontend"
log_msg "Frontend: lint..."
npm run lint || die "npm run lint failed"
log_msg "Frontend: type-check..."
npm run type-check || die "npm run type-check failed"
log_msg "Frontend: test..."
npm test || die "npm test failed"
log_msg "Frontend: build..."
npm run build || die "npm run build failed"
cd "${ROOT_DIR}"

log_ok "All quality gates passed."

log_msg "\n--- SUCCESS SUMMARY ---"
log_msg "  * Prerequisite validation passed"
log_msg "  * Backend dependencies installed"
log_msg "  * Frontend dependencies installed"
log_msg "  * Postgres healthy"
log_msg "  * Redis healthy"
log_msg "  * Migrations applied/current"
log_msg "  * /health passed"
log_msg "  * /ready passed"
log_msg "  * Backend Ruff/mypy/pytest passed"
log_msg "  * Frontend lint/type/test/build passed"
log_msg "  * Core Docker services intentionally left running"
log_msg "\nBootstrap complete."
exit 0
