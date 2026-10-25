#!/usr/bin/env bash
# Zero-prerequisite launcher for machines that have Docker but not `make`.
# Usage: ./run.sh            # start the stack (2 streamers, 3 collectors)
#        ./run.sh down       # stop and delete all data
set -euo pipefail
cd "$(dirname "$0")"

# Find the docker CLI even if it is not on PATH (e.g. right after Docker Desktop install).
DOCKER="$(command -v docker || echo "$HOME/.docker/bin/docker")"
"$DOCKER" version >/dev/null 2>&1 || { echo "Docker is not running. Start Docker Desktop and retry."; exit 1; }

if [ "${1:-up}" = "down" ]; then
  "$DOCKER" compose down -v
  exit 0
fi

# Generate a random DB password once (never hardcoded, never committed).
[ -f .env ] || { umask 077; echo "POSTGRES_PASSWORD=$(openssl rand -hex 24)" > .env; }

"$DOCKER" compose up --build -d --scale streamer="${STREAMERS:-2}" --scale collector="${COLLECTORS:-3}"
echo
echo "Stack is starting. In a few seconds:"
echo "  API:     http://localhost:8080/api/v1/gpus"
echo "  Swagger: http://localhost:8080/swagger/index.html"
echo "Stop with: ./run.sh down"
