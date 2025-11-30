#!/usr/bin/env python3
"""
Example 08: Performance Optimization
====================================

Learn Spark performance optimization techniques:
- Understanding partitions
- Broadcast joins
- Avoiding shuffles
- Caching strategies
- Optimizing file formats

Run: python src/examples/08_performance.py
     OR: make run-perf
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, broadcast, spark_partition_id, 
    count, sum as spark_sum, avg, monotonically_increasing_id
)
from pyspark.storagelevel import StorageLevel
import time


def main():
    print("\n" + "="*60)
    print("  EXAMPLE 08: Performance Optimization")
    print("="*60 + "\n")
    
    spark = (
        SparkSession.builder
        .appName("PerformanceOptimization")
        .master("local[4]")  # Use 4 cores
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    # ===========================================
    # 1. UNDERSTANDING PARTITIONS
    # ===========================================
    print("--- Understanding Partitions ---\n")
    
    # Create sample data
    large_df = spark.range(1000000).withColumn("value", col("id") * 2)
    
    print(f"Default partitions: {large_df.rdd.getNumPartitions()}")
    
    # See partition distribution
    partition_counts = (
        large_df
        .withColumn("partition_id", spark_partition_id())
        .groupBy("partition_id")
        .count()
        .orderBy("partition_id")
    )
    
    print("Records per partition:")
    partition_counts.show()
    
    # Repartition
    repartitioned = large_df.repartition(4)
    print(f"After repartition(4): {repartitioned.rdd.getNumPartitions()} partitions")
    
    # Coalesce (reduce partitions without full shuffle)
    coalesced = large_df.coalesce(2)
    print(f"After coalesce(2): {coalesced.rdd.getNumPartitions()} partitions")
    print()
    
    # ===========================================
    # 2. BROADCAST JOINS
    # ===========================================
    print("--- Broadcast Joins ---\n")
    print("""
When to use broadcast joins:
- One table is small (< 10MB by default)
- Avoid shuffle of large table
- Small table fits in memory of all executors
""")
    
    # Large table
    orders = spark.range(100000).select(
        col("id").alias("order_id"),
        (col("id") % 1000).alias("customer_id"),
        (col("id") * 10).alias("amount")
    )
    
    # Small lookup table
    customers = spark.range(1000).select(
        col("id").alias("customer_id"),
        lit("Customer").alias("name")
    )
    
    print(f"Orders: {orders.count()} rows")
    print(f"Customers: {customers.count()} rows")
    
    # Regular join (may shuffle both sides)
    start_time = time.time()
    regular_join = orders.join(customers, "customer_id")
    _ = regular_join.count()
    regular_time = time.time() - start_time
    print(f"\nRegular join time: {regular_time:.3f}s")
    
    # Broadcast join (small table sent to all executors)
    start_time = time.time()
    broadcast_join = orders.join(broadcast(customers), "customer_id")
    _ = broadcast_join.count()
    broadcast_time = time.time() - start_time
    print(f"Broadcast join time: {broadcast_time:.3f}s")
    
    # Show explain plans
    print("\nRegular join plan:")
    regular_join.explain(mode="simple")
    
    print("\nBroadcast join plan:")
    broadcast_join.explain(mode="simple")
    print()
    
    # ===========================================
    # 3. AVOIDING SHUFFLES
    # ===========================================
    print("--- Avoiding Shuffles ---\n")
    print("""
Operations that cause shuffles:
- groupBy, agg
- join (non-broadcast)
- repartition
- distinct
- orderBy

Ways to reduce shuffles:
- Use broadcast joins for small tables
- Pre-partition data for repeated operations
- Use repartition + join on same key
- Avoid unnecessary distinct/groupBy
""")
    
    # Bad: Multiple shuffles
    print("Bad pattern - multiple shuffles:")
    bad_df = (
        spark.range(100000)
        .withColumn("category", col("id") % 10)
        .withColumn("value", col("id") * 2)
        .groupBy("category").count()  # Shuffle 1
        .orderBy("count")  # Shuffle 2
    )
    bad_df.explain(mode="simple")
    
    # Better: Pre-partition
    print("\nBetter pattern - pre-partition for multiple operations:")
    pre_partitioned = (
        spark.range(100000)
        .withColumn("category", col("id") % 10)
        .withColumn("value", col("id") * 2)
        .repartition("category")  # Single partition
    )
    
    # Multiple operations now shuffle-free within each partition
    stats1 = pre_partitioned.groupBy("category").count()
    stats2 = pre_partitioned.groupBy("category").agg(spark_sum("value"))
    print(f"Stats1 partitions: {stats1.rdd.getNumPartitions()}")
    print(f"Stats2 partitions: {stats2.rdd.getNumPartitions()}")
    print()
    
    # ===========================================
    # 4. CACHING STRATEGIES
    # ===========================================
    print("--- Caching Strategies ---\n")
    print("""
Storage Levels:
- MEMORY_ONLY: Store in JVM heap as objects
- MEMORY_AND_DISK: Spill to disk if needed
- MEMORY_ONLY_SER: Serialized (less memory, more CPU)
- DISK_ONLY: Only on disk

When to cache:
- Repeated access to same DataFrame
- After expensive transformations
- Before iterative algorithms

When NOT to cache:
- One-time use DataFrames
- Very large datasets (may cause OOM)
- Simple transformations
""")
    
    # Create a DataFrame with expensive computation
    expensive_df = (
        spark.range(1000000)
        .withColumn("complex", col("id") * col("id") + col("id") / 2)
        .filter(col("id") % 2 == 0)
    )
    
    # Without caching - recomputes each time
    print("Without caching:")
    start_time = time.time()
    _ = expensive_df.count()
    print(f"  First count: {time.time() - start_time:.3f}s")
    
    start_time = time.time()
    _ = expensive_df.count()
    print(f"  Second count: {time.time() - start_time:.3f}s (recomputed)")
    
    # With caching
    print("\nWith caching:")
    cached_df = expensive_df.cache()
    
    start_time = time.time()
    _ = cached_df.count()  # Triggers computation and caching
    print(f"  First count: {time.time() - start_time:.3f}s (computed + cached)")
    
    start_time = time.time()
    _ = cached_df.count()  # Uses cache
    print(f"  Second count: {time.time() - start_time:.3f}s (from cache)")
    
    # Different storage levels
    print("\nStorage level examples:")
    
    # Memory only
    df_memory = expensive_df.persist(StorageLevel.MEMORY_ONLY)
    print("  MEMORY_ONLY: Fast but may OOM")
    
    # Memory and disk
    df_disk = expensive_df.persist(StorageLevel.MEMORY_AND_DISK)
    print("  MEMORY_AND_DISK: Safe, spills to disk")
    
    # Unpersist when done
    cached_df.unpersist()
    df_memory.unpersist()
    df_disk.unpersist()
    print("\n  ✓ Unpersisted cached DataFrames")
    print()
    
    # ===========================================
    # 5. OPTIMIZING FILE FORMATS
    # ===========================================
    print("--- Optimizing File Formats ---\n")
    print("""
Format Comparison:
┌─────────────┬──────────────┬─────────────┬─────────────┐
│ Format      │ Compression  │ Read Speed  │ Use Case    │
├─────────────┼──────────────┼─────────────┼─────────────┤
│ Parquet     │ Excellent    │ Fast        │ Analytics   │
│ ORC         │ Excellent    │ Fast        │ Hive/Spark  │
│ Avro        │ Good         │ Medium      │ Streaming   │
│ JSON        │ None         │ Slow        │ Schema flex │
│ CSV         │ None         │ Slow        │ Interchange │
└─────────────┴──────────────┴─────────────┴─────────────┘

Parquet optimizations:
- Column pruning (only read needed columns)
- Predicate pushdown (filter at read time)
- Partition pruning (skip irrelevant partitions)
""")
    
    # Demonstrate column pruning
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "output", "perf_test"
    )
    os.makedirs(output_dir, exist_ok=True)
    
    # Create and save sample data
    sample_df = spark.range(100000).select(
        col("id"),
        (col("id") * 2).alias("col2"),
        (col("id") * 3).alias("col3"),
        (col("id") * 4).alias("col4"),
        (col("id") * 5).alias("col5"),
        (col("id") % 10).alias("partition_col")
    )
    
    parquet_path = os.path.join(output_dir, "sample.parquet")
    sample_df.write.mode("overwrite").partitionBy("partition_col").parquet(parquet_path)
    print(f"Saved partitioned parquet to: {parquet_path}")
    
    # Column pruning example
    print("\nColumn pruning - only reads needed columns:")
    pruned = spark.read.parquet(parquet_path).select("id", "col2")
    pruned.explain(mode="simple")
    
    # Predicate pushdown
    print("\nPredicate pushdown - filters at read time:")
    filtered = spark.read.parquet(parquet_path).filter(col("id") > 90000)
    filtered.explain(mode="simple")
    
    # Partition pruning
    print("\nPartition pruning - only reads relevant partitions:")
    partition_filtered = spark.read.parquet(parquet_path).filter(col("partition_col") == 5)
    partition_filtered.explain(mode="simple")
    print()
    
    # ===========================================
    # 6. CONFIGURATION TIPS
    # ===========================================
    print("--- Configuration Tips ---\n")
    print("""
Key Spark configurations:

Memory:
  spark.driver.memory=4g          # Driver memory
  spark.executor.memory=8g        # Executor memory
  spark.memory.fraction=0.6       # Fraction for execution/storage

Parallelism:
  spark.sql.shuffle.partitions=200   # Partitions after shuffle
  spark.default.parallelism=100      # Default RDD partitions
  spark.sql.files.maxPartitionBytes  # Max bytes per partition

Adaptive Query Execution (AQE) - Spark 3.0+:
  spark.sql.adaptive.enabled=true
  spark.sql.adaptive.coalescePartitions.enabled=true
  spark.sql.adaptive.skewJoin.enabled=true

Broadcast:
  spark.sql.autoBroadcastJoinThreshold=10485760  # 10MB default
""")
    
    # Show current configuration
    print("Current configuration:")
    configs = [
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.driver.memory"
    ]
    
    for config in configs:
        value = spark.conf.get(config, "not set")
        print(f"  {config}: {value}")
    print()
    
    # ===========================================
    # 7. COMMON ANTI-PATTERNS
    # ===========================================
    print("--- Common Anti-Patterns ---\n")
    print("""
❌ Anti-patterns to avoid:

1. Collecting large datasets to driver:
   BAD:  df.collect()  # OOM risk!
   GOOD: df.take(100) or df.limit(100).toPandas()

2. Using UDFs when built-in functions exist:
   BAD:  udf(lambda x: x.upper())
   GOOD: upper(col("name"))

3. Repeated actions without caching:
   BAD:  df.count(); df.show()  # Recomputes
   GOOD: df.cache(); df.count(); df.show()

4. Too many small files:
   BAD:  df.write.parquet(path)  # Many partitions
   GOOD: df.coalesce(10).write.parquet(path)

5. Cartesian products:
   BAD:  df1.crossJoin(df2)  # Explodes data
   GOOD: Use proper join conditions

6. Not filtering early:
   BAD:  df.join(other).filter(...)
   GOOD: df.filter(...).join(other.filter(...))
""")
    
    # ===========================================
    # CLEANUP
    # ===========================================
    spark.stop()
    print("="*60)
    print("  Performance Optimization Complete! 🎉")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
