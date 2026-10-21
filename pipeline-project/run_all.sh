#!/usr/bin/env bash
# Run the whole pipeline locally (local[*]) or on the cluster: SPARK_MASTER=spark://spark-master:7077 ./run_all.sh
set -euo pipefail
cd "$(dirname "$0")"
[ -d data/raw ] || python data/generate_reviews.py
mkdir -p output
for s in 01_ingest 02_clean_flatten 03_features 04_analytics; do
  echo "=== $s ==="; python pipeline/$s.py
done
python pipeline/05_dashboard.py
python visual-demo/build_data.py   # refresh the interactive demo (visual-demo/index.html)
