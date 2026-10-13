"""STEP 4 - ANALYTICS: descriptive statistics from gold -> small CSVs for the dashboard step."""
import json
from pyspark.sql import functions as F
from common import *

spark = get_spark("04-analytics")
spark.sparkContext.setLogLevel("WARN")
r = spark.read.parquet(f"{GOLD}/reviews").filter(F.col("rating").isNotNull())
h = spark.read.parquet(f"{GOLD}/hotel_stats")
cm = spark.read.parquet(f"{GOLD}/city_month")

q1, med, q3 = r.approxQuantile("rating", [.25, .5, .75], 0.01)
summary = {"n_rated_reviews": r.count(), "mean_rating": round(r.agg(F.avg("rating")).first()[0], 3),
           "q1": q1, "median": med, "q3": q3,
           "corr_rating_sentiment": round(r.stat.corr("rating", "sentiment"), 3)}
print(json.dumps(summary, indent=2)); json.dump(summary, open(f"{OUT}/summary.json", "w"))

to_csv(r.groupBy("city").agg(F.count("*").alias("n"), F.round(F.avg("rating"), 3).alias("avg_rating"),
                             F.round(F.avg("sentiment"), 4).alias("avg_sentiment")).orderBy("city"), f"{OUT}/city_summary.csv")
to_csv(r.groupBy("rating").count().orderBy("rating"), f"{OUT}/rating_dist.csv")
to_csv(r.groupBy("label").agg(F.round(F.avg("rating"), 3).alias("avg_rating"), F.count("*").alias("n")), f"{OUT}/sentiment_vs_rating.csv")
to_csv(cm.groupBy("month").agg(F.sum("n_reviews").alias("n_reviews")).orderBy("month"), f"{OUT}/monthly_volume.csv")
to_csv(h.filter(F.col("n_reviews") >= 30).orderBy(F.desc("avg_rating")).limit(10), f"{OUT}/top_hotels.csv")
print("CSV outputs written to", OUT)
