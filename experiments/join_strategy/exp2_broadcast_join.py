"""
Experiment 2 — Broadcast Join Optimization
============================================
Built on:
    Exp1 (Parquet already applied)

Isolated variable:
    Broadcast users + products only

Control:
    shuffle.partitions = 200
    No caching
    No partition pruning
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, broadcast
import time

spark = SparkSession.builder \
    .appName("Exp2 Broadcast Join") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("  EXPERIMENT 2 — Broadcast Join")
print("=" * 60)
print("  File Format        : Parquet")
print("  Shuffle Partitions : 200")
print("  Broadcast          : users + products")
print("=" * 60)

timings = {}
job_start = time.time()

# ─────────────────────────────────────────────
# STAGE 1 — Read Parquet
# ─────────────────────────────────────────────
print("\n[Stage 1] Reading Parquet files...")
t0 = time.time()

orders   = spark.read.parquet("data/parquet/orders.parquet")
users    = spark.read.parquet("data/parquet/users.parquet")
products = spark.read.parquet("data/parquet/products.parquet")

orders.count()
users.count()
products.count()

timings["read_seconds"] = round(time.time() - t0, 2)
print(f"  Time: {timings['read_seconds']}s")

# ─────────────────────────────────────────────
# STAGE 2 — Broadcast Join
# ─────────────────────────────────────────────
print("\n[Stage 2] Joining with broadcast hints...")
t0 = time.time()

products_clean = products.drop("category")

joined = orders \
    .join(broadcast(users), on="user_id", how="inner") \
    .join(broadcast(products_clean), on="product_id", how="inner")

joined.count()

timings["join_seconds"] = round(time.time() - t0, 2)
print(f"  Time: {timings['join_seconds']}s")

# ─────────────────────────────────────────────
# STAGE 3 — Aggregation + Sort
# ─────────────────────────────────────────────
print("\n[Stage 3] Aggregation + sort...")
t0 = time.time()

result = joined \
    .groupBy("order_date", "category", "country") \
    .agg(_sum("amount").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc())

result.count()

timings["aggregation_seconds"] = round(time.time() - t0, 2)
print(f"  Time: {timings['aggregation_seconds']}s")

# ─────────────────────────────────────────────
# STAGE 4 — Write Parquet
# ─────────────────────────────────────────────
print("\n[Stage 4] Writing Parquet...")
t0 = time.time()

result.write \
    .mode("overwrite") \
    .parquet("exp2_output")

timings["write_seconds"] = round(time.time() - t0, 2)
print(f"  Time: {timings['write_seconds']}s")

timings["wall_clock_total"] = round(time.time() - job_start, 2)

print("\n" + "=" * 60)
print("  EXPERIMENT 2 RESULTS")
print("=" * 60)
print(f"  Read        : {timings['read_seconds']}s")
print(f"  Join        : {timings['join_seconds']}s")
print(f"  Aggregation : {timings['aggregation_seconds']}s")
print(f"  Write       : {timings['write_seconds']}s")
print(f"  TOTAL       : {timings['wall_clock_total']}s")
print("=" * 60)

spark.stop()
