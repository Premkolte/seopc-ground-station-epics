#!/usr/bin/env bash
# =============================================
# ARGUS MISSION CONTROL - Launch Script
# Usage: ./launch.sh [--rebuild]
# =============================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SESSION="argus"

# --- Rebuild flag ---
REBUILD=""
if [[ "$1" == "--rebuild" ]]; then
    REBUILD="--build"
    echo "[ARGUS] Rebuilding containers..."
fi

# ── PHASE 1: Infrastructure ─────────────────────────────────────────────────
echo "[ARGUS] Starting infrastructure (redpanda, minio, postgis, prometheus, grafana)..."
sudo docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d $REBUILD \
    redpanda minio postgis prometheus grafana

echo "[ARGUS] Waiting for PostGIS to be ready..."
until sudo docker exec postgis pg_isready -U admin -d seopc_metadata -q 2>/dev/null; do
    sleep 1
done
echo "[ARGUS] PostGIS ready."

# ── PHASE 2: Application containers ─────────────────────────────────────────
echo "[ARGUS] Starting satellite and processor..."
sudo docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d $REBUILD \
    satellite processor
echo "[ARGUS] All containers up."

# ── PHASE 3: tmux layout ─────────────────────────────────────────────────────
# Kill old session safely
tmux kill-session -t "$SESSION" 2>/dev/null

sleep 1  # let tmux server fully shut down

# Create new detached session (no forced dimensions — let terminal decide)
tmux new-session -d -s "$SESSION"

# Build layout:
#   [  docker logs (top-left)  |  Go dashboard (top-right)  ]
#   [  GUI log    (bot-left)   |  tile feeder  (bot-right)  ]

# Split top pane left|right (right column = 60% of width)
tmux split-window -h -t "$SESSION:0.0" -p 60

# Split left column top|bottom (bottom = 40% of left height)
tmux split-window -v -t "$SESSION:0.0" -p 40

# Split right column top|bottom (bottom = 35% of right height)
tmux split-window -v -t "$SESSION:0.1" -p 35

# ── PHASE 4: Staggered commands ──────────────────────────────────────────────

# Pane 0 — Docker logs (immediate, read-only)
tmux send-keys -t "$SESSION:0.0" \
    "sudo docker compose -f $PROJECT_DIR/docker-compose.yml logs -f processor satellite" Enter

# Pane 2 — GUI (starts after 5s; graphical window opens separately, logs silenced)
tmux send-keys -t "$SESSION:0.2" \
    "echo '[GUI] Starting in 5s...' && sleep 5 && cd $PROJECT_DIR/gui && ./visualizer >>/tmp/argus_gui.log 2>&1; echo '[GUI] Window closed. Logs: /tmp/argus_gui.log'" Enter

# Pane 1 — Dashboard (starts after 8s, DB is fully settled)
tmux send-keys -t "$SESSION:0.1" \
    "echo '[DASH] Starting in 8s...' && sleep 8 && cd $PROJECT_DIR/dashboard && go run main.go" Enter

# Pane 3 — Tile feeder (starts last, processor needs time to warm up)
tmux send-keys -t "$SESSION:0.3" \
    "echo '[FEEDER] Starting in 12s...' && sleep 12 && cd $PROJECT_DIR && bash feed.sh" Enter

# ── Attach ────────────────────────────────────────────────────────────────────
echo "[ARGUS] Attaching to tmux session '$SESSION'..."
tmux attach-session -t "$SESSION"
