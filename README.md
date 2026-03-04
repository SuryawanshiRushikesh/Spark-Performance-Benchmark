# ⚡ Spark Performance Optimization Lab

A structured Spark performance engineering project focused on 
systematically identifying and eliminating bottlenecks through
controlled experimentation.

---

## 📌 Objective

Reduce Spark job runtime using architectural and configuration-level
optimizations — without changing hardware.

Baseline workload was intentionally unoptimized to simulate
real-world inefficiencies.

---

## 🧪 Baseline

| Configuration | Value |
|--------------|--------|
| File Format | CSV |
| Shuffle Partitions | 200 |
| Broadcast | ❌ |
| Partitioning | ❌ |

**Total Runtime: 20.36 seconds**

---

## 🚀 Optimization Experiments

### 1️⃣ File Format (CSV → Parquet)
- Reduced scan cost using columnar storage  
- Runtime: **15.69s**
- ~23% improvement  

---

### 2️⃣ Broadcast Join
- Broadcast dimension tables (users, products)
- Runtime: **14.27s**
- ~9% improvement  

---

### 3️⃣ Partition Pruning
- Used partitioned Parquet (order_date)
- Filtered selective workload
- Runtime: **4.98s**
- ~62% faster than full scan  

---

### 4️⃣ Shuffle Partition Tuning
- Reduced shuffle partitions from 200 → 25
- Runtime: **3.16s**
- ~73% faster vs default configuration  

---

## 🏁 Final Optimized Runtime

| Stage | Runtime |
|-------|---------|
| Baseline | 20.36s |
| Optimized | 3.16s |

**Total Improvement: ~84% reduction**

---

## 🧠 Key Learnings

- Storage format impacts performance more than compute tweaks.
- Data layout (partitioning) is critical for selective workloads.
- Broadcast joins help when shuffle is dominant.
- Over-parallelization can degrade performance.
- Performance tuning must be data-driven.

---

## 📂 Project Structure

```
baseline/
experiments/
analysis/
reports/
```

---

## 💡 Takeaway

This project demonstrates systematic Spark optimization through:

- Controlled experiments
- Isolated variable testing
- Measured performance impact
- Architectural thinking

No hardware changes.  
Only intelligent design.

---

⚡ Built as a focused Spark performance specialization project.
