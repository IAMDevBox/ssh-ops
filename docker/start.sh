#!/bin/bash
cd "$(dirname "$0")"

echo "Building and starting SSH test nodes..."
docker compose up -d --build

echo ""
echo "Waiting for SSH services..."
sleep 2

# Health check
for port in 2201 2202 2203; do
  if nc -z 127.0.0.1 $port 2>/dev/null; then
    echo "  ✓ 127.0.0.1:$port ready"
  else
    echo "  ✗ 127.0.0.1:$port not ready"
  fi
done

echo ""
echo "Test nodes:"
echo "  dev-node1   127.0.0.1:2201  admin/admin123 (sudo)"
echo "  dev-node2   127.0.0.1:2202  admin/admin123 (sudo)"
echo "  prod-app1   127.0.0.1:2203  admin/admin123 (sudo) [PROD warning]"
echo ""
echo "Config: config/docker-test.yml"
echo "Stop:   docker/stop.sh"
