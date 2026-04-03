#!/usr/bin/env bash
# =============================================
# ARGUS TILE FEEDER
# Automatically drip-feeds satellite tiles
# from tiles/ into downlink_buffer/
# Usage: bash feed.sh [delay_seconds]
# =============================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TILES_DIR="$PROJECT_DIR/tiles"
BUFFER_DIR="$PROJECT_DIR/downlink_buffer"
DELAY="${1:-5}"  # Default: one tile every 5 seconds

mkdir -p "$BUFFER_DIR"

echo "=========================================="
echo "  ARGUS TILE FEEDER"
echo "  Source : $TILES_DIR"
echo "  Buffer : $BUFFER_DIR"
echo "  Delay  : ${DELAY}s between tiles"
echo "  Press Ctrl+C to stop."
echo "=========================================="

# Collect all tile files, sorted
TILES=()
while IFS= read -r line; do
    [[ -n "$line" ]] && TILES+=("$line")
done < <(find "$TILES_DIR" -maxdepth 1 \( -iname "*.jpg" -o -iname "*.jpeg" -o -iname "*.png" \) | sort)
TOTAL="${#TILES[@]}"

if [[ "$TOTAL" -eq 0 ]]; then
    echo "[ERROR] No .jpg tiles found in $TILES_DIR"
    exit 1
fi

echo "[FEEDER] Found $TOTAL tiles to process."
echo ""

INDEX=0
while true; do
    TILE="${TILES[$INDEX]}"
    FILENAME="$(basename "$TILE")"

    echo "[FEEDER] >> Sending tile $((INDEX + 1))/$TOTAL : $FILENAME"

    # Use docker cp into the container's filesystem directly.
    # Plain cp to a Docker volume does NOT trigger inotify inside the container on WSL2.
    CONTAINER=$(docker ps --filter "name=satellite" --format "{{.Names}}" | head -1)
    if [[ -z "$CONTAINER" ]]; then
        echo "[FEEDER] WARNING: satellite container not found. Falling back to cp."
        rm -f "$BUFFER_DIR/$FILENAME"
        sleep 0.1
        cp "$TILE" "$BUFFER_DIR/$FILENAME"
    else
        docker cp "$TILE" "$CONTAINER:/downlink_buffer/$FILENAME"
    fi

    INDEX=$(( (INDEX + 1) % TOTAL ))

    # Pause at end of all tiles
    if [[ "$INDEX" -eq 0 ]]; then
        echo "[FEEDER] -- All tiles sent. Looping from start..."
    fi

    sleep "$DELAY"
done
