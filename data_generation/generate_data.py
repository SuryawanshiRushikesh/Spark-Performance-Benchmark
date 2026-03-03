"""
E-Commerce Data Generator for Spark Optimization Project
=========================================================
Generates 3 tables:
  - orders     (1,000,000 rows) — fact table with time-based skew
  - users      (100,000 rows)   — dimension table
  - products   (10,000 rows)    — dimension table

Output:
  - CSV format
  - Parquet format (flat)
  - Partitioned Parquet by order_date (for orders)

Time Skew Strategy:
  - Weekends get 3x more orders
  - Black Friday / Cyber Monday get 10x more orders
  - Christmas week gets 5x more orders
  - Regular weekdays get baseline volume
"""

import os
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
ORDERS_COUNT   = 1_000_000
USERS_COUNT    = 100_000
PRODUCTS_COUNT = 10_000

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

OUTPUT_BASE = "./data"

DIRS = {
    "csv"       : f"{OUTPUT_BASE}/csv",
    "parquet"   : f"{OUTPUT_BASE}/parquet",
    "partitioned": f"{OUTPUT_BASE}/partitioned_parquet",
}

for path in DIRS.values():
    os.makedirs(path, exist_ok=True)

print("=" * 60)
print("  E-Commerce Data Generator — Spark Optimization Project")
print("=" * 60)


# ─────────────────────────────────────────────
# HELPER — TIME SKEW WEIGHT FUNCTION
# ─────────────────────────────────────────────
def get_date_weight(date: datetime) -> float:
    """
    Assigns a weight to each date based on realistic e-commerce traffic patterns.
    Higher weight = more orders generated on that day.
    """
    month, day, weekday = date.month, date.day, date.weekday()

    # Black Friday (last Friday of November)
    if month == 11 and weekday == 4 and day >= 23:
        return 10.0

    # Cyber Monday (Monday after Black Friday)
    if month == 11 and weekday == 0 and day >= 26:
        return 9.0

    # Christmas week
    if month == 12 and 20 <= day <= 26:
        return 5.0

    # New Year's Eve / Day
    if (month == 12 and day == 31) or (month == 1 and day == 1):
        return 4.0

    # Valentine's Day week
    if month == 2 and 10 <= day <= 14:
        return 3.0

    # Weekends
    if weekday >= 5:
        return 3.0

    # Regular weekday
    return 1.0


# ─────────────────────────────────────────────
# STEP 1 — GENERATE DATE POOL WITH SKEW
# ─────────────────────────────────────────────
print("\n[1/4] Building date pool with time-based skew...")

start_date = datetime(2023, 1, 1)
end_date   = datetime(2024, 12, 31)

date_range = [start_date + timedelta(days=i)
              for i in range((end_date - start_date).days + 1)]

weights = np.array([get_date_weight(d) for d in date_range])
weights = weights / weights.sum()  # normalize to probabilities

sampled_dates = np.random.choice(date_range, size=ORDERS_COUNT, p=weights)
sampled_dates = [d.strftime("%Y-%m-%d") for d in sampled_dates]

print(f"    Date pool built. Sample distribution:")
date_series = pd.Series(sampled_dates)
top_dates = date_series.value_counts().head(5)
for date, count in top_dates.items():
    print(f"      {date}  →  {count:,} orders")


# ─────────────────────────────────────────────
# STEP 2 — GENERATE users TABLE
# ─────────────────────────────────────────────
print("\n[2/4] Generating users table...")

genders   = ["Male", "Female", "Non-binary", "Prefer not to say"]
countries = [
    "USA", "UK", "Germany", "France", "Canada",
    "India", "Australia", "Brazil", "Japan", "Mexico"
]

users_signup_start = datetime(2018, 1, 1)
users_signup_end   = datetime(2023, 12, 31)
signup_days        = (users_signup_end - users_signup_start).days

users_df = pd.DataFrame({
    "user_id": [f"U{str(i).zfill(6)}" for i in range(1, USERS_COUNT + 1)],
    "age": np.random.randint(18, 70, size=USERS_COUNT),
    "gender": np.random.choice(genders, size=USERS_COUNT,
                               p=[0.48, 0.48, 0.02, 0.02]),
    "country": np.random.choice(countries, size=USERS_COUNT,
                                p=[0.35, 0.12, 0.10, 0.08, 0.08,
                                   0.10, 0.06, 0.05, 0.04, 0.02]),
    "signup_date": [
        (users_signup_start + timedelta(days=random.randint(0, signup_days))).strftime("%Y-%m-%d")
        for _ in range(USERS_COUNT)
    ],
})

print(f"    users table: {len(users_df):,} rows | {users_df.shape[1]} columns")


# ─────────────────────────────────────────────
# STEP 3 — GENERATE products TABLE
# ─────────────────────────────────────────────
print("\n[3/4] Generating products table...")

categories = [
    "Electronics", "Clothing", "Home & Kitchen",
    "Books", "Sports", "Beauty", "Toys", "Automotive"
]

brands_by_category = {
    "Electronics"   : ["Samsung", "Apple", "Sony", "LG", "Bose", "Dell", "Asus"],
    "Clothing"      : ["Nike", "Adidas", "Zara", "H&M", "Levi's", "Puma"],
    "Home & Kitchen": ["IKEA", "Cuisinart", "KitchenAid", "Dyson", "Philips"],
    "Books"         : ["Penguin", "HarperCollins", "Random House", "Oxford"],
    "Sports"        : ["Nike", "Adidas", "Under Armour", "Wilson", "Spalding"],
    "Beauty"        : ["L'Oreal", "Maybelline", "MAC", "Clinique", "Dove"],
    "Toys"          : ["LEGO", "Hasbro", "Mattel", "Fisher-Price"],
    "Automotive"    : ["Bosch", "3M", "Michelin", "Castrol"],
}

price_ranges = {
    "Electronics"   : (50,  2000),
    "Clothing"      : (10,   300),
    "Home & Kitchen": (15,   500),
    "Books"         : (5,     60),
    "Sports"        : (10,   400),
    "Beauty"        : (5,    150),
    "Toys"          : (5,    200),
    "Automotive"    : (10,   500),
}

product_categories = np.random.choice(categories, size=PRODUCTS_COUNT)

products_df = pd.DataFrame({
    "product_id": [f"P{str(i).zfill(5)}" for i in range(1, PRODUCTS_COUNT + 1)],
    "category"  : product_categories,
    "price"     : [
        round(random.uniform(*price_ranges[cat]), 2)
        for cat in product_categories
    ],
    "brand": [
        random.choice(brands_by_category[cat])
        for cat in product_categories
    ],
})

print(f"    products table: {len(products_df):,} rows | {products_df.shape[1]} columns")


# ─────────────────────────────────────────────
# STEP 4 — GENERATE orders TABLE
# ─────────────────────────────────────────────
print("\n[4/4] Generating orders table (1M rows with time skew)...")

payment_types = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Crypto"]
cities = [
    "New York", "Los Angeles", "Chicago", "Houston", "London",
    "Berlin", "Paris", "Toronto", "Sydney", "Mumbai",
    "Tokyo", "São Paulo", "Mexico City", "Dubai", "Singapore"
]

user_ids    = users_df["user_id"].values
product_ids = products_df["product_id"].values

# Join product info for realistic order data
product_lookup = products_df.set_index("product_id")[["category", "price"]].to_dict("index")

sampled_product_ids = np.random.choice(product_ids, size=ORDERS_COUNT)

orders_df = pd.DataFrame({
    "order_id"    : [f"O{str(i).zfill(7)}" for i in range(1, ORDERS_COUNT + 1)],
    "user_id"     : np.random.choice(user_ids, size=ORDERS_COUNT),
    "product_id"  : sampled_product_ids,
    "category"    : [product_lookup[pid]["category"] for pid in sampled_product_ids],
    "amount"      : [
        round(product_lookup[pid]["price"] * random.uniform(0.8, 1.3), 2)
        for pid in sampled_product_ids
    ],
    "quantity"    : np.random.choice([1, 1, 1, 2, 2, 3, 4, 5],
                                      size=ORDERS_COUNT),
    "order_date"  : sampled_dates,
    "city"        : np.random.choice(cities, size=ORDERS_COUNT),
    "payment_type": np.random.choice(payment_types, size=ORDERS_COUNT,
                                      p=[0.35, 0.25, 0.20, 0.08, 0.08, 0.04]),
})

print(f"    orders table: {len(orders_df):,} rows | {orders_df.shape[1]} columns")
print(f"\n    Category distribution:")
cat_dist = orders_df["category"].value_counts()
for cat, cnt in cat_dist.items():
    pct = cnt / ORDERS_COUNT * 100
    print(f"      {cat:<20} {cnt:>8,}  ({pct:.1f}%)")


# ─────────────────────────────────────────────
# SAVE — CSV
# ─────────────────────────────────────────────
print("\n[Saving] CSV format...")
orders_df.to_csv(  f"{DIRS['csv']}/orders.csv",   index=False)
users_df.to_csv(   f"{DIRS['csv']}/users.csv",    index=False)
products_df.to_csv(f"{DIRS['csv']}/products.csv", index=False)
print("    CSV saved ✓")


# ─────────────────────────────────────────────
# SAVE — PARQUET (flat)
# ─────────────────────────────────────────────
print("[Saving] Parquet format (flat)...")
orders_df.to_parquet(  f"{DIRS['parquet']}/orders.parquet",   index=False)
users_df.to_parquet(   f"{DIRS['parquet']}/users.parquet",    index=False)
products_df.to_parquet(f"{DIRS['parquet']}/products.parquet", index=False)
print("    Parquet saved ✓")


# ─────────────────────────────────────────────
# SAVE — PARTITIONED PARQUET (orders only)
# ─────────────────────────────────────────────
print("[Saving] Partitioned Parquet (orders by order_date)...")
orders_df.to_parquet(
    f"{DIRS['partitioned']}/orders",
    index=False,
    partition_cols=["order_date"],
)
print("    Partitioned Parquet saved ✓")


# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("  DATA GENERATION COMPLETE")
print("=" * 60)

def get_size(path):
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp):
                total += os.path.getsize(fp)
    return round(total / (1024 * 1024), 2)

print(f"\n  Output sizes:")
print(f"    CSV format          : {get_size(DIRS['csv'])} MB")
print(f"    Parquet (flat)      : {get_size(DIRS['parquet'])} MB")
print(f"    Partitioned Parquet : {get_size(DIRS['partitioned'])} MB")

print(f"\n  Tables:")
print(f"    orders   → {len(orders_df):>10,} rows")
print(f"    users    → {len(users_df):>10,} rows")
print(f"    products → {len(products_df):>10,} rows")

print(f"\n  Skew type: TIME-BASED (peak days)")
print(f"    Weekends        → 3x baseline volume")
print(f"    Black Friday    → 10x baseline volume")
print(f"    Cyber Monday    → 9x baseline volume")
print(f"    Christmas week  → 5x baseline volume")
print(f"    Valentine's week→ 3x baseline volume")

print("\n  Ready for Spark optimization experiments! 🚀")
print("=" * 60)
