"""STEP 1 - INGEST: raw JSON lines -> bronze Parquet (+ quarantine for unparseable lines)."""
import json
from pyspark.sql import functions as F
from pyspark.sql.types import *
from common import *

spark = get_spark("01-ingest")
spark.sparkContext.setLogLevel("WARN")

# Explicit schema. rating is read as STRING on purpose: the source mixes 4.5, "4.5", "N/A", null.
review = StructType([
    StructField("review_id", StringType()), StructField("user", StringType()),
    StructField("rating", StringType()), StructField("text", StringType()),
    StructField("date", StringType()), StructField("tags", ArrayType(StringType())),
    StructField("helpful_votes", IntegerType())])
schema = StructType([
    StructField("hotel_id", StringType()), StructField("name", StringType()),
    StructField("location", StructType([StructField("city", StringType()),
                                        StructField("lat", DoubleType()), StructField("lon", DoubleType())])),
    StructField("stars", IntegerType()), StructField("reviews", ArrayType(review)),
    StructField("_corrupt", StringType())])

raw = (spark.read.schema(schema)
       .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt")
       .json(RAW).cache())                      # cache: Spark disallows querying only the corrupt column otherwise

total = raw.count()
bad = raw.filter(F.col("_corrupt").isNotNull())
good = raw.filter(F.col("_corrupt").isNull()).drop("_corrupt")
n_bad = bad.count()
bad.select("_corrupt").write.mode("overwrite").json(QUAR)       # keep rejects for inspection
(good.withColumn("_ingested_at", F.current_timestamp())
     .withColumn("_source_file", F.input_file_name())
     .write.mode("overwrite").parquet(BRONZE))
print(f"lines read={total}  good={total-n_bad}  quarantined={n_bad}")
json.dump({"lines_read": total, "lines_quarantined": n_bad}, open(f"{OUT}/dq_ingest.json", "w"))
