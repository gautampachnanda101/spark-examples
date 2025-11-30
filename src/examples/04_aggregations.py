#!/usr/bin/env python3
"""
Example 04: Aggregations
========================

Master aggregation operations in Spark:
- Basic aggregations (count, sum, avg, min, max)
- Group by operations
- Multiple aggregations
- Pivot tables
- Rollup and Cube

Run: python src/examples/04_aggregations.py
     OR: make run-agg
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    countDistinct, collect_list, collect_set, first, last,
    approx_count_distinct, stddev, variance, percentile_approx,
    round as spark_round, lit, when
)


def main():
    print("\n" + "="*60)
    print("  EXAMPLE 04: Aggregations")
    print("="*60 + "\n")
    
    spark = (
        SparkSession.builder
        .appName("Aggregations")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    # ===========================================
    # 1. CREATE SAMPLE DATA
    # ===========================================
    print("--- Creating Sample Data ---\n")
    
    # Sales data
    sales = spark.createDataFrame([
        ("2023-01", "Electronics", "North", "Widget A", 100, 25.00),
        ("2023-01", "Electronics", "North", "Widget B", 150, 35.00),
        ("2023-01", "Electronics", "South", "Widget A", 80, 25.00),
        ("2023-01", "Clothing", "North", "Shirt", 200, 15.00),
        ("2023-01", "Clothing", "South", "Shirt", 180, 15.00),
        ("2023-02", "Electronics", "North", "Widget A", 120, 25.00),
        ("2023-02", "Electronics", "North", "Widget B", 130, 35.00),
        ("2023-02", "Electronics", "South", "Widget A", 90, 25.00),
        ("2023-02", "Clothing", "North", "Pants", 100, 40.00),
        ("2023-02", "Clothing", "South", "Shirt", 160, 15.00),
        ("2023-03", "Electronics", "North", "Widget A", 140, 25.00),
        ("2023-03", "Electronics", "South", "Widget B", 110, 35.00),
        ("2023-03", "Clothing", "North", "Shirt", 220, 15.00),
        ("2023-03", "Clothing", "South", "Pants", 90, 40.00)
    ], ["month", "category", "region", "product", "quantity", "unit_price"])
    
    # Add revenue column
    sales = sales.withColumn("revenue", col("quantity") * col("unit_price"))
    
    print("Sales data:")
    sales.show()
    
    # ===========================================
    # 2. BASIC AGGREGATIONS
    # ===========================================
    print("--- Basic Aggregations ---\n")
    
    # Single aggregation
    print("Total revenue:")
    sales.select(
        spark_sum("revenue").alias("total_revenue")
    ).show()
    
    # Multiple aggregations
    print("Multiple aggregations on entire dataset:")
    sales.select(
        count("*").alias("num_records"),
        countDistinct("product").alias("unique_products"),
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("revenue").alias("total_revenue"),
        spark_round(avg("revenue"), 2).alias("avg_revenue"),
        spark_min("revenue").alias("min_revenue"),
        spark_max("revenue").alias("max_revenue")
    ).show()
    
    # Using agg() with column expressions
    print("Using agg():")
    sales.agg(
        count("*").alias("records"),
        spark_sum("revenue").alias("total"),
        avg("quantity").alias("avg_qty")
    ).show()
    
    # ===========================================
    # 3. GROUP BY
    # ===========================================
    print("--- Group By Operations ---\n")
    
    # Single column groupby
    print("Revenue by category:")
    sales.groupBy("category").agg(
        count("*").alias("num_sales"),
        spark_sum("quantity").alias("total_qty"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue")
    ).show()
    
    # Multiple column groupby
    print("Revenue by category and region:")
    sales.groupBy("category", "region").agg(
        spark_sum("quantity").alias("total_qty"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue")
    ).orderBy("category", "region").show()
    
    # Groupby with multiple aggregations
    print("Comprehensive stats by category:")
    category_stats = sales.groupBy("category").agg(
        count("*").alias("num_transactions"),
        countDistinct("product").alias("unique_products"),
        spark_sum("quantity").alias("total_qty"),
        spark_round(avg("quantity"), 1).alias("avg_qty"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        spark_round(avg("revenue"), 2).alias("avg_revenue"),
        spark_min("revenue").alias("min_sale"),
        spark_max("revenue").alias("max_sale")
    )
    category_stats.show()
    
    # ===========================================
    # 4. CONDITIONAL AGGREGATIONS
    # ===========================================
    print("--- Conditional Aggregations ---\n")
    
    # Conditional count
    print("Conditional counts:")
    sales.groupBy("category").agg(
        count("*").alias("total_sales"),
        count(when(col("region") == "North", True)).alias("north_sales"),
        count(when(col("region") == "South", True)).alias("south_sales"),
        spark_round(
            spark_sum(when(col("region") == "North", col("revenue")).otherwise(0)),
            2
        ).alias("north_revenue")
    ).show()
    
    # ===========================================
    # 5. COLLECT FUNCTIONS
    # ===========================================
    print("--- Collect Functions ---\n")
    
    # Collect values into arrays
    print("Products per category:")
    sales.groupBy("category").agg(
        collect_set("product").alias("unique_products"),
        collect_list("product").alias("all_products")
    ).show(truncate=False)
    
    # First and Last
    print("First and last products sold:")
    sales.groupBy("category").agg(
        first("product").alias("first_product"),
        last("product").alias("last_product"),
        first("revenue").alias("first_revenue"),
        last("revenue").alias("last_revenue")
    ).show()
    
    # ===========================================
    # 6. STATISTICAL AGGREGATIONS
    # ===========================================
    print("--- Statistical Aggregations ---\n")
    
    # Create numeric dataset
    numbers = spark.createDataFrame([
        ("A", 10), ("A", 20), ("A", 30), ("A", 15), ("A", 25),
        ("B", 100), ("B", 110), ("B", 90), ("B", 105), ("B", 95)
    ], ["group", "value"])
    
    print("Statistical measures:")
    numbers.groupBy("group").agg(
        count("value").alias("n"),
        spark_round(avg("value"), 2).alias("mean"),
        spark_round(stddev("value"), 2).alias("stddev"),
        spark_round(variance("value"), 2).alias("variance"),
        spark_min("value").alias("min"),
        spark_max("value").alias("max"),
        percentile_approx("value", 0.5).alias("median")
    ).show()
    
    # ===========================================
    # 7. PIVOT TABLES
    # ===========================================
    print("--- Pivot Tables ---\n")
    
    # Basic pivot - revenue by category and region
    print("Pivot: Revenue by category (rows) and region (columns):")
    pivot_df = sales.groupBy("category").pivot("region").agg(
        spark_round(spark_sum("revenue"), 2)
    )
    pivot_df.show()
    
    # Pivot with explicit values (better performance)
    print("Pivot with explicit values:")
    pivot_explicit = (
        sales
        .groupBy("category")
        .pivot("region", ["North", "South"])
        .agg(spark_round(spark_sum("revenue"), 2))
    )
    pivot_explicit.show()
    
    # Pivot by month
    print("Revenue by category over months:")
    monthly_pivot = (
        sales
        .groupBy("category")
        .pivot("month", ["2023-01", "2023-02", "2023-03"])
        .agg(spark_round(spark_sum("revenue"), 2))
    )
    monthly_pivot.show()
    
    # Multiple aggregations in pivot
    print("Multiple aggs in pivot (quantity and revenue):")
    multi_pivot = (
        sales
        .groupBy("category")
        .pivot("region")
        .agg(
            spark_sum("quantity").alias("qty"),
            spark_round(spark_sum("revenue"), 2).alias("rev")
        )
    )
    multi_pivot.show()
    
    # ===========================================
    # 8. ROLLUP AND CUBE
    # ===========================================
    print("--- Rollup and Cube ---\n")
    
    # ROLLUP - hierarchical subtotals
    print("ROLLUP (category -> region):")
    rollup_df = (
        sales
        .rollup("category", "region")
        .agg(
            spark_sum("quantity").alias("total_qty"),
            spark_round(spark_sum("revenue"), 2).alias("total_revenue")
        )
        .orderBy("category", "region")
    )
    rollup_df.show()
    
    # CUBE - all combinations of subtotals
    print("CUBE (all combinations):")
    cube_df = (
        sales
        .cube("category", "region")
        .agg(
            spark_sum("quantity").alias("total_qty"),
            spark_round(spark_sum("revenue"), 2).alias("total_revenue")
        )
        .orderBy("category", "region")
    )
    cube_df.show()
    
    # ===========================================
    # 9. APPROXIMATE AGGREGATIONS
    # ===========================================
    print("--- Approximate Aggregations (for big data) ---\n")
    
    # Generate larger dataset for demo
    large_data = spark.range(100000).select(
        (col("id") % 100).alias("user_id"),
        (col("id") % 10).alias("category")
    )
    
    # Exact vs Approximate count distinct
    print("Exact vs Approximate distinct count:")
    large_data.select(
        countDistinct("user_id").alias("exact_distinct"),
        approx_count_distinct("user_id").alias("approx_distinct")
    ).show()
    
    print("Note: approx_count_distinct is much faster on large datasets!")
    
    # ===========================================
    # CLEANUP
    # ===========================================
    spark.stop()
    print("\n" + "="*60)
    print("  Aggregations Complete! 🎉")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
