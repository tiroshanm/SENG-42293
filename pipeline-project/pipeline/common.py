import os
from pyspark.sql import SparkSession

ROOT = os.environ.get("PROJECT_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
RAW = f"{ROOT}/data/raw"
BRONZE = f"{ROOT}/lake/bronze"
SILVER = f"{ROOT}/lake/silver"
GOLD = f"{ROOT}/lake/gold"
QUAR = f"{ROOT}/lake/quarantine"
OUT = f"{ROOT}/output"

def get_spark(app):
    """local[*] by default; set SPARK_MASTER=spark://spark-master:7077 to use the cluster."""
    master = os.environ.get("SPARK_MASTER", "local[*]")
    return (SparkSession.builder.appName(app).master(master)
            .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "8"))
            .getOrCreate())

def to_csv(df, path):
    """Write a SMALL Spark DataFrame to one CSV on the driver (no pandas needed inside the cluster image)."""
    import csv
    rows = df.collect()
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(df.columns); w.writerows([list(r) for r in rows])
