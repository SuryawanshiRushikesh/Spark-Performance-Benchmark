"""
Experiment 4 — Shuffle Partition Tuning
========================================
Goal:
    Find optimal shuffle partition count for pruned workload.

Built on:
    Parquet
    Broadcast joins
    Partition pruning (single date filter)

We test:
    200 (default)
    100
    50
    25

Fix vs naive approach:
    Cannot create multiple SparkSessions in a loop on local.
    getOrCreate() returns existing session and ignores new config.
    Instead we use spark.conf.set() to reconfigure between runs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, broadcast
import time

FILTER_DATE = "2023-03-15"
PARTITION_TESTS = [200, 100, 50, 25]

results = {}

print("=" * 60)
print("  EXPERIMENT 4 — Shuffle Partition Tuning")
print("=" * 60)
print(f"  Filter Date : {FILTER_DATE}")
print(f"  Testing     : {PARTITION_TESTS} partitions")
print("=" * 60)

# Single session — reconfigure between runs
spark = SparkSession.builder \
    .appName("Exp4 Shuffle Partition Tuning") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

for partitions in PARTITION_TESTS:

    # Reconfigure shuffle partitions for this run
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))

    print(f"\n[Testing] shuffle.partitions = {partitions}...")
    t0 = time.time()

    orders = spark.read.parquet("data/partitioned_parquet/orders") \
        .filter(col("order_date") == FILTER_DATE)

    users         = spark.read.parquet("data/parquet/users.parquet")
    products      = spark.read.parquet("data/parquet/products.parquet")
    products_clean = products.drop("category")

    joined = orders \
        .join(broadcast(users),          on="user_id") \
        .join(broadcast(products_clean), on="product_id")

    result = joined \
        .groupBy("order_date", "category", "country") \
        .agg(_sum("amount").alias("total_revenue"))

    result.count()

    total_time = round(time.time() - t0, 2)
    results[partitions] = total_time

    print(f"  Time: {total_time}s")

# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────
best    = min(results, key=results.get)
worst   = max(results, key=results.get)

print("\n" + "=" * 60)
print("  SHUFFLE TUNING RESULTS")
print("=" * 60)
print(f"  {'Partitions':<15} {'Time':>10} {'vs 200 partitions':>18}")
print(f"  {'─'*15} {'─'*10} {'─'*18}")

baseline_time = results[200]
for p in PARTITION_TESTS:
    diff = round(((baseline_time - results[p]) / baseline_time) * 100, 1)
    flag = " ← BEST" if p == best else ""
    print(f"  {p:<15} {results[p]:>9}s {diff:>17}%{flag}")

print(f"  {'─'*15} {'─'*10} {'─'*18}")
print(f"  Best config : {best} partitions → {results[best]}s")
print(f"  Worst config: {worst} partitions → {results[worst]}s")
print("=" * 60)
print(f"\n  All experiments complete! Time to analyze results 🚀")
print("=" * 60)

spark.stop()

