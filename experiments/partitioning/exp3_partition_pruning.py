"""
Experiment 3 — Partition Pruning
=================================
Hypothesis:
    Filtering on partition column (order_date)
    will drastically reduce scan time.

Built on:
    Parquet format + Broadcast joins (Exp2)

Comparison:
    A) Full table scan (1M rows)
    B) Single weekday filter (partition pruning)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, broadcast
import time

spark = SparkSession.builder \
    .appName("Exp3 Partition Pruning") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("  EXPERIMENT 3 — Partition Pruning")
print("=" * 60)
print("  File Format        : Partitioned Parquet")
print("  Broadcast          : users + products")
print("  Shuffle Partitions : 200")
print("=" * 60)

# Pick a normal weekday (low traffic)
FILTER_DATE = "2023-03-15"

# ─────────────────────────────────────────────
# COMMON DIM TABLE READ
# ─────────────────────────────────────────────
users    = spark.read.parquet("data/parquet/users.parquet")
products = spark.read.parquet("data/parquet/products.parquet")
products_clean = products.drop("category")

# ─────────────────────────────────────────────
# CASE A — FULL SCAN
# ─────────────────────────────────────────────
print("\n[Case A] Full table scan (no filter)...")
t0 = time.time()

orders_full = spark.read.parquet("data/partitioned_parquet/orders")

joined_full = orders_full \
    .join(broadcast(users), on="user_id") \
    .join(broadcast(products_clean), on="product_id")

result_full = joined_full \
    .groupBy("order_date", "category", "country") \
    .agg(_sum("amount").alias("total_revenue"))

result_full.count()

full_time = round(time.time() - t0, 2)
print(f"  Full scan time: {full_time}s")

# ─────────────────────────────────────────────
# CASE B — PARTITION PRUNED SCAN
# ─────────────────────────────────────────────
print(f"\n[Case B] Partition pruning on date = {FILTER_DATE} ...")
t0 = time.time()

orders_pruned = spark.read.parquet("data/partitioned_parquet/orders") \
    .filter(col("order_date") == FILTER_DATE)

joined_pruned = orders_pruned \
    .join(broadcast(users), on="user_id") \
    .join(broadcast(products_clean), on="product_id")

result_pruned = joined_pruned \
    .groupBy("order_date", "category", "country") \
    .agg(_sum("amount").alias("total_revenue"))

result_pruned.count()

pruned_time = round(time.time() - t0, 2)
print(f"  Pruned scan time: {pruned_time}s")

# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("  PARTITION PRUNING RESULTS")
print("=" * 60)
print(f"  Full Scan   : {full_time}s")
print(f"  Pruned Scan : {pruned_time}s")

if full_time > 0:
    improvement = round(((full_time - pruned_time) / full_time) * 100, 1)
    print(f"  Improvement : {improvement}% faster")
print("=" * 60)

spark.stop()
