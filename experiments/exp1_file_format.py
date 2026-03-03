"""
Experiment 1 — File Format Optimization (CSV → Parquet)
=========================================================
Hypothesis:
    Switching from CSV to Parquet will significantly reduce
    read time due to columnar storage and compression.

Isolated variable:
    ONLY the file format changes.
    Everything else is identical to baseline.

Control:
    shuffle.partitions = 200 (default)
    No broadcast hints
    No caching
    No partition pruning

Expected outcome:
    CSV read was 9.29s → Parquet should cut this by 50-70%
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import time
import json
import os

# ─────────────────────────────────────────────
# SPARK SESSION — identical to baseline
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Exp1 File Format") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

os.makedirs("benchmark_results", exist_ok=True)

print("=" * 60)
print("  EXPERIMENT 1 — File Format (CSV → Parquet)")
print("=" * 60)
print(f"  shuffle.partitions : 200 (unchanged)")
print(f"  file format        : PARQUET ← changed")
print(f"  broadcast hint     : None (unchanged)")
print(f"  caching            : None (unchanged)")
print("=" * 60)

timings = {}

# ─────────────────────────────────────────────
# STAGE 1 — Read Parquet (only change vs baseline)
# ─────────────────────────────────────────────
print("\n[Stage 1] Reading Parquet files...")
t0 = time.time()

orders   = spark.read.parquet("data/parquet/orders.parquet")
users    = spark.read.parquet("data/parquet/users.parquet")
products = spark.read.parquet("data/parquet/products.parquet")

orders_count   = orders.count()
users_count    = users.count()
products_count = products.count()

timings["parquet_read_seconds"] = round(time.time() - t0, 2)
print(f"  orders   → {orders_count:,} rows")
print(f"  users    → {users_count:,} rows")
print(f"  products → {products_count:,} rows")
print(f"  Time: {timings['parquet_read_seconds']}s  (baseline was 9.29s)")

# ─────────────────────────────────────────────
# STAGE 2 — Join (identical to baseline)
# ─────────────────────────────────────────────
print("\n[Stage 2] Joining tables (unchanged)...")
t0 = time.time()

products_clean = products.drop("category")

joined = orders \
    .join(users,          on="user_id",    how="inner") \
    .join(products_clean, on="product_id", how="inner")

join_count = joined.count()

timings["join_seconds"] = round(time.time() - t0, 2)
print(f"  Joined rows → {join_count:,}")
print(f"  Time: {timings['join_seconds']}s  (baseline was 1.95s)")

# ─────────────────────────────────────────────
# STAGE 3 — Aggregation + Sort (identical to baseline)
# ─────────────────────────────────────────────
print("\n[Stage 3] Running aggregation + sort (unchanged)...")
t0 = time.time()

result = joined \
    .groupBy("order_date", "category", "country") \
    .agg(_sum("amount").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc())

result_count = result.count()

timings["aggregation_seconds"] = round(time.time() - t0, 2)
print(f"  Result rows → {result_count:,}")
print(f"  Time: {timings['aggregation_seconds']}s  (baseline was 3.41s)")

# ─────────────────────────────────────────────
# STAGE 4 — Write Parquet (changed from CSV)
# ─────────────────────────────────────────────
print("\n[Stage 4] Writing results as Parquet...")
t0 = time.time()

result.write \
    .mode("overwrite") \
    .parquet("benchmark_results/exp1_output")

timings["write_seconds"] = round(time.time() - t0, 2)
print(f"  Time: {timings['write_seconds']}s  (baseline was 5.71s)")

# ─────────────────────────────────────────────
# SUMMARY + COMPARISON
# ─────────────────────────────────────────────
timings["total_seconds"] = round(sum(timings.values()), 2)

BASELINE = {
    "read" : 9.29,
    "join" : 1.95,
    "agg"  : 3.41,
    "write": 5.71,
    "total": 20.36,
}

def improvement(before, after):
    return round(((before - after) / before) * 100, 1)

print("\n" + "=" * 60)
print("  EXPERIMENT 1 RESULTS vs BASELINE")
print("=" * 60)
print(f"  {'Stage':<15} {'Baseline':>10} {'Exp1':>10} {'Improvement':>12}")
print(f"  {'─'*15} {'─'*10} {'─'*10} {'─'*12}")
print(f"  {'Read':<15} {BASELINE['read']:>9}s {timings['parquet_read_seconds']:>9}s {improvement(BASELINE['read'], timings['parquet_read_seconds']):>10}%")
print(f"  {'Join':<15} {BASELINE['join']:>9}s {timings['join_seconds']:>9}s {improvement(BASELINE['join'], timings['join_seconds']):>10}%")
print(f"  {'Aggregation':<15} {BASELINE['agg']:>9}s {timings['aggregation_seconds']:>9}s {improvement(BASELINE['agg'], timings['aggregation_seconds']):>10}%")
print(f"  {'Write':<15} {BASELINE['write']:>9}s {timings['write_seconds']:>9}s {improvement(BASELINE['write'], timings['write_seconds']):>10}%")
print(f"  {'─'*15} {'─'*10} {'─'*10} {'─'*12}")
print(f"  {'TOTAL':<15} {BASELINE['total']:>9}s {timings['total_seconds']:>9}s {improvement(BASELINE['total'], timings['total_seconds']):>10}%")
print("=" * 60)

# Save timings as JSON
with open("benchmark_results/exp1_timings.json", "w") as f:
    json.dump({
        "job"    : "exp1_file_format",
        "config" : {
            "file_format"        : "Parquet",
            "shuffle_partitions" : 200,
            "broadcast_hint"     : False,
            "caching"            : False,
            "partition_pruning"  : False,
        },
        "timings"   : timings,
        "baseline"  : BASELINE,
        "improvement_pct": improvement(BASELINE["total"], timings["total_seconds"]),
        "row_counts": {
            "orders"  : orders_count,
            "users"   : users_count,
            "products": products_count,
            "joined"  : join_count,
            "result"  : result_count,
        }
    }, f, indent=2)

print(f"\n  Timings saved → benchmark_results/exp1_timings.json")
print(f"  Next → Exp 2: Broadcast Join 🚀")
print("=" * 60)

spark.stop()

