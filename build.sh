#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="timescaleui"
IMAGE_TAG="${1:-latest}"

echo "Building ${IMAGE_NAME}:${IMAGE_TAG}..."
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .
echo "Done. Image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo ""
echo "Use in your docker-compose.yml:"
echo ""
echo "  tsui:"
echo "    image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "    ports:"
echo '      - "8080:8080"'
echo "    environment:"
echo '      TSUI_LISTEN_ADDR: ":8080"'
echo '      TSUI_DATA_DIR: "/data"'
echo "    volumes:"
echo "      - tsui-data:/data"
