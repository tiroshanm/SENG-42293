# Travel-Review Big Data Pipeline (SENG 42293)

Raw nested JSON -> quarantine bad lines -> flatten & clean -> Parquet (bronze/silver/gold) -> features -> analytics -> charts.

## Quick start (local, no Docker)
```bash
python3 -m venv .venv && source .venv/bin/activate   # Python 3.10-3.13 work with Spark 3.5 (setuptools is in requirements.txt)
pip install -r requirements.txt                       # needs Java 17 (or 11/21) installed
./run_all.sh
```
Outputs: `lake/` (Parquet layers), `output/` (CSV summaries + PNG charts).

## Multi-node run (Docker)
```bash
./run_cluster.sh        # master + 2 workers, then open http://localhost:8080
```
The compose file was not executed in the authoring environment - test it before class.

## Layout
- `data/generate_reviews.py` - seeded generator of messy nested JSON (`data/raw/` is already generated)
- `pipeline/01_ingest.py ... 05_dashboard.py` - one script per stage; `common.py` holds paths + SparkSession
- `docker-compose.yml`, `run_all.sh`, `run_cluster.sh`
- `visual-demo/` - interactive control-room UI that animates the pipeline and explores its outputs (open `visual-demo/index.html`)

The data is synthetic. Replace `data/raw/` with a licensed real dataset (e.g. a public hotel-review dump) for the semester project; respect site terms before scraping anything.
