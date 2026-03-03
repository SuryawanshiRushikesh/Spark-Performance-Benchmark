"""
Baseline Spark Job — Unoptimized
=================================
Intentionally uses:
  - CSV format (slowest)
  - Default shuffle partitions (200)
  - No broadcast hints
  - No caching
  - No partition pruning

This establishes the BEFORE benchmark.
Every optimization experiment will be compared against these numbers.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import time
import json
import os

# ─────────────────────────────────────────────
# SPARK SESSION — Default config, no tuning
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Baseline Spark Job") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

os.makedirs("benchmark_results", exist_ok=True)

print("=" * 60)
print("  BASELINE BENCHMARK — Unoptimized")
print("=" * 60)
print(f"  shuffle.partitions : 200 (default)")
print(f"  file format        : CSV")
print(f"  broadcast hint     : None")
print(f"  caching            : None")
print("=" * 60)

timings = {}

# ─────────────────────────────────────────────
# STAGE 1 — Read CSV
# ─────────────────────────────────────────────
print("\n[Stage 1] Reading CSV files...")
t0 = time.time()

orders   = spark.read.csv("data/csv/orders.csv",   header=True, inferSchema=True)
users    = spark.read.csv("data/csv/users.csv",    header=True, inferSchema=True)
products = spark.read.csv("data/csv/products.csv", header=True, inferSchema=True)

# Force evaluation
orders_count   = orders.count()
users_count    = users.count()
products_count = products.count()

timings["csv_read_seconds"] = round(time.time() - t0, 2)
print(f"  orders   → {orders_count:,} rows")
print(f"  users    → {users_count:,} rows")
print(f"  products → {products_count:,} rows")
print(f"  Time: {timings['csv_read_seconds']}s")

# ─────────────────────────────────────────────
# STAGE 2 — Joins (unoptimized shuffle joins)
# ─────────────────────────────────────────────
print("\n[Stage 2] Joining tables (unoptimized shuffle join)...")
t0 = time.time()

# Drop ambiguous category column from products before joining
products_clean = products.drop("category")

joined = orders \
    .join(users,           on="user_id",    how="inner") \
    .join(products_clean,  on="product_id", how="inner")

join_count = joined.count()

timings["join_seconds"] = round(time.time() - t0, 2)
print(f"  Joined rows → {join_count:,}")
print(f"  Time: {timings['join_seconds']}s")

# ─────────────────────────────────────────────
# STAGE 3 — Heavy Aggregation + Sort
# ─────────────────────────────────────────────
print("\n[Stage 3] Running aggregation + sort...")
t0 = time.time()

result = joined \
    .groupBy("order_date", "category", "country") \
    .agg(_sum("amount").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc())

result_count = result.count()

timings["aggregation_seconds"] = round(time.time() - t0, 2)
print(f"  Result rows → {result_count:,}")
print(f"  Time: {timings['aggregation_seconds']}s")

# ─────────────────────────────────────────────
# STAGE 4 — Save Results
# ─────────────────────────────────────────────
print("\n[Stage 4] Saving results...")
t0 = time.time()

result.write \
    .mode("overwrite") \
    .csv("benchmark_results/baseline_output", header=True)

timings["write_seconds"] = round(time.time() - t0, 2)
print(f"  Time: {timings['write_seconds']}s")

# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────
timings["total_seconds"] = round(sum(timings.values()), 2)

print("\n" + "=" * 60)
print("  BASELINE RESULTS")
print("=" * 60)
print(f"  CSV Read       : {timings['csv_read_seconds']}s")
print(f"  Join           : {timings['join_seconds']}s")
print(f"  Aggregation    : {timings['aggregation_seconds']}s")
print(f"  Write          : {timings['write_seconds']}s")
print(f"  ─────────────────────────────")
print(f"  TOTAL          : {timings['total_seconds']}s")
print("=" * 60)

# Save timings as JSON for later comparison
with open("benchmark_results/baseline_timings.json", "w") as f:
    json.dump({
        "job"     : "baseline",
        "config"  : {
            "file_format"         : "CSV",
            "shuffle_partitions"  : 200,
            "broadcast_hint"      : False,
            "caching"             : False,
            "partition_pruning"   : False,
        },
        "timings" : timings,
        "row_counts": {
            "orders"   : orders_count,
            "users"    : users_count,
            "products" : products_count,
            "joined"   : join_count,
            "result"   : result_count,
        }
    }, f, indent=2)

print("\n  Timings saved → benchmark_results/baseline_timings.json")
print("  This is your BEFORE number. Now we optimize. 🚀")
print("=" * 60)

spark.stop()

