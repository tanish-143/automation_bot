#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# Market Scanner — Local Development Startup Script
# ═══════════════════════════════════════════════════════════════════════
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "═══════════════════════════════════════════════════════════════"
echo "  Market Scanner — Starting Local Dev Environment"
echo "═══════════════════════════════════════════════════════════════"

# ── Step 1: Start infrastructure (TimescaleDB + Redis) ──────────────
echo ""
echo "▸ Starting TimescaleDB and Redis via Docker Compose..."
docker compose up -d db redis
echo "  ✓ Waiting for services to be healthy..."
sleep 5

# Wait for DB to be ready (up to 30s)
for i in $(seq 1 30); do
  if docker compose exec -T db pg_isready -U scanner >/dev/null 2>&1; then
    echo "  ✓ TimescaleDB is ready"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "  ✗ TimescaleDB did not become ready in 30s"
    exit 1
  fi
  sleep 1
done

# Wait for Redis
for i in $(seq 1 15); do
  if docker compose exec -T redis redis-cli ping >/dev/null 2>&1; then
    echo "  ✓ Redis is ready"
    break
  fi
  if [ "$i" -eq 15 ]; then
    echo "  ✗ Redis did not become ready in 15s"
    exit 1
  fi
  sleep 1
done

# ── Step 2: Install Python backend dependencies ────────────────────
echo ""
echo "▸ Installing Python backend dependencies..."
cd "$PROJECT_DIR/backend"
pip install -r requirements.txt --quiet 2>&1 | tail -3
echo "  ✓ Backend dependencies installed"

# ── Step 3: Install frontend dependencies ──────────────────────────
echo ""
echo "▸ Installing frontend dependencies..."
cd "$PROJECT_DIR/frontend"
npm install --silent 2>&1 | tail -3
echo "  ✓ Frontend dependencies installed"

# ── Step 4: Start the backend API server ───────────────────────────
echo ""
echo "▸ Starting FastAPI backend on http://localhost:8000 ..."
cd "$PROJECT_DIR/backend"
export PYTHONPATH="$PROJECT_DIR/backend:$PROJECT_DIR"
uvicorn api:app --host 0.0.0.0 --port 8000 --reload --reload-dir "$PROJECT_DIR/backend" &
BACKEND_PID=$!
echo "  ✓ Backend started (PID: $BACKEND_PID)"

# ── Step 5: Start the frontend dev server ──────────────────────────
echo ""
echo "▸ Starting Vite frontend on http://localhost:5173 ..."
cd "$PROJECT_DIR/frontend"
npm run dev -- --host 0.0.0.0 &
FRONTEND_PID=$!
echo "  ✓ Frontend started (PID: $FRONTEND_PID)"

# ── Summary ────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  ✅ All services started!"
echo ""
echo "  Frontend:  http://localhost:5173"
echo "  Backend:   http://localhost:8000"
echo "  API Docs:  http://localhost:8000/docs"
echo "  Health:    http://localhost:8000/health"
echo "  DB:        localhost:5432 (user: scanner, pass: scanner)"
echo "  Redis:     localhost:6379"
echo ""
echo "  Press Ctrl+C to stop all services"
echo "═══════════════════════════════════════════════════════════════"

# Trap Ctrl+C to clean up
cleanup() {
  echo ""
  echo "▸ Stopping services..."
  kill $BACKEND_PID 2>/dev/null || true
  kill $FRONTEND_PID 2>/dev/null || true
  echo "  ✓ Backend and frontend stopped"
  echo "  ℹ  DB and Redis still running. Stop with: docker compose down"
}
trap cleanup EXIT INT TERM

# Wait for background processes
wait
