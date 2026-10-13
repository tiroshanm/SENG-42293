#!/usr/bin/env bash
# Run steps 1-4 on the Docker Spark cluster, then draw charts on the host.
set -euo pipefail
cd "$(dirname "$0")"
docker compose up -d
sleep 8   # let the workers register with the master
for s in 01_ingest 02_clean_flatten 03_features 04_analytics; do
  echo "=== $s (cluster) ==="
  docker compose exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 /opt/project/pipeline/$s.py
done
python pipeline/05_dashboard.py
