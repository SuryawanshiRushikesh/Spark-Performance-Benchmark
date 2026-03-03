from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("Spark Sanity Check") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── CSV ────────────────────────────────────────
print("\nReading CSV...")
t0 = time.time()
orders_csv = spark.read.csv("data/csv/orders.csv", header=True, inferSchema=True)
users_csv = spark.read.csv("data/csv/users.csv", header=True, inferSchema=True)
products_csv = spark.read.csv("data/csv/products.csv", header=True, inferSchema=True)
print(f"  orders   → {orders_csv.count():,} rows")
print(f"  users    → {users_csv.count():,} rows")
print(f"  products → {products_csv.count():,} rows")
print(f"  CSV read time: {time.time() - t0:.2f}s")

# ── Parquet ────────────────────────────────────
print("\nReading Parquet...")
t0 = time.time()
orders_pq = spark.read.parquet("data/parquet/orders.parquet")
users_pq = spark.read.parquet("data/parquet/users.parquet")
products_pq = spark.read.parquet("data/parquet/products.parquet")
print(f"  orders   → {orders_pq.count():,} rows")
print(f"  users    → {users_pq.count():,} rows")
print(f"  products → {products_pq.count():,} rows")
print(f"  Parquet read time: {time.time() - t0:.2f}s")

# ── Schema Check ───────────────────────────────
print("\nOrders Schema:")
orders_pq.printSchema()

# ── Sample ─────────────────────────────────────
print("\nSample rows:")
orders_pq.show(5, truncate=False)

spark.stop()
print("\nSanity check passed!")

