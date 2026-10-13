"""STEP 2 - CLEAN & FLATTEN: bronze -> silver (one row per review, validated, de-duplicated)."""
import json
from pyspark.sql import functions as F
from common import *

spark = get_spark("02-clean-flatten")
spark.sparkContext.setLogLevel("WARN")
bronze = spark.read.parquet(BRONZE)

# 2a. flatten: explode the reviews array, lift nested fields to columns
flat = (bronze.select("hotel_id", F.col("name").alias("hotel_name"), "stars",
                      F.col("location.city").alias("city"), F.explode("reviews").alias("r"))
        .select("hotel_id", "hotel_name", "stars", "city", "r.*"))
n_flat = flat.count()

# 2b. de-duplicate on the business key (exact duplicates were injected in the source)
dedup = flat.dropDuplicates(["review_id"])
n_dedup = dedup.count()

# 2c. type repair + validation
rating = F.col("rating").cast("double")                     # "N/A" -> null, "4.5" -> 4.5
d1 = F.to_date("date", "yyyy-MM-dd"); d2 = F.to_date("date", "dd/MM/yyyy")
silver = (dedup
    .withColumn("rating_raw", F.col("rating"))
    .withColumn("rating", F.when(rating.between(1, 5), rating))                 # out-of-range -> null
    .withColumn("review_date", F.coalesce(d1, d2))                              # two date formats
    .withColumn("text", F.trim(F.col("text")))
    .withColumn("has_tags", F.col("tags").isNotNull())
    .drop("date"))
silver.write.mode("overwrite").partitionBy("city").parquet(SILVER)

s = spark.read.parquet(SILVER)
stats = {"reviews_flattened": n_flat, "after_dedup": n_dedup,
         "rating_missing_or_invalid": s.filter(F.col("rating").isNull()).count(),
         "date_unparsed": s.filter(F.col("review_date").isNull()).count(),
         "silver_rows": s.count()}
print(json.dumps(stats, indent=2))
json.dump(stats, open(f"{OUT}/dq_clean.json", "w"))
