"""STEP 3 - FEATURES: silver -> gold (review-level features + hotel and city-month aggregates)."""
from pyspark.sql import functions as F
from common import *

POS = ["great", "friendly", "clean", "excellent", "peaceful", "comfortable", "amazing", "lovely", "good", "recommended"]
NEG = ["noisy", "dirty", "rude", "slow", "overpriced", "uncomfortable", "poor", "cold", "disappointing", "not"]

spark = get_spark("03-features")
spark.sparkContext.setLogLevel("WARN")
s = spark.read.parquet(SILVER)

words = F.split(F.regexp_replace(F.lower("text"), r"[^a-z\s]", ""), r"\s+")
f = (s.withColumn("words", words)
      .withColumn("n_words", F.size("words"))
      .withColumn("pos_hits", F.size(F.filter("words", lambda w: w.isin(POS))))
      .withColumn("neg_hits", F.size(F.filter("words", lambda w: w.isin(NEG))))
      .withColumn("sentiment", (F.col("pos_hits") - F.col("neg_hits")) / F.greatest(F.col("n_words"), F.lit(1)))
      .withColumn("label", F.when(F.col("sentiment") > 0, "positive").when(F.col("sentiment") < 0, "negative").otherwise("neutral"))
      .withColumn("year", F.year("review_date")).withColumn("month", F.date_format("review_date", "yyyy-MM"))
      .withColumn("weekday", F.date_format("review_date", "E"))
      .drop("words"))
f.write.mode("overwrite").parquet(f"{GOLD}/reviews")

rated = f.filter(F.col("rating").isNotNull())
hotel = (rated.groupBy("hotel_id", "hotel_name", "city", "stars")
         .agg(F.count("*").alias("n_reviews"), F.round(F.avg("rating"), 3).alias("avg_rating"),
              F.round(F.avg("sentiment"), 4).alias("avg_sentiment"),
              F.round(F.avg((F.col("label") == "positive").cast("int")), 3).alias("pct_positive"),
              F.max("review_date").alias("last_review")))
hotel.write.mode("overwrite").parquet(f"{GOLD}/hotel_stats")

citym = (rated.groupBy("city", "month").agg(F.count("*").alias("n_reviews"), F.round(F.avg("rating"), 3).alias("avg_rating")))
citym.write.mode("overwrite").parquet(f"{GOLD}/city_month")
print("gold tables written:", f.count(), "review rows;", hotel.count(), "hotels;", citym.count(), "city-months")
