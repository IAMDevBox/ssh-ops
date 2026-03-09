#!/bin/bash
cd "$(dirname "$0")"

echo "Stopping SSH test nodes..."
docker compose down

echo "Done."
