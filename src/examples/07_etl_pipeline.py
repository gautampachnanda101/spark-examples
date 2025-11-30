#!/usr/bin/env python3
"""
Example 07: ETL Pipeline
========================

Build a complete ETL pipeline in Spark:
- Extract: Read from multiple sources
- Transform: Clean, enrich, aggregate data
- Load: Write to various formats

Run: python src/examples/07_etl_pipeline.py
     OR: make run-etl
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, trim, upper, lower, 
    regexp_replace, to_date, year, month, quarter, dayofweek,
    sum as spark_sum, avg, count, countDistinct,
    round as spark_round, current_timestamp,
    row_number, broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, TimestampType
)
import shutil
from datetime import date


def main():
    print("\n" + "="*60)
    print("  EXAMPLE 07: ETL Pipeline")
    print("="*60 + "\n")
    
    spark = (
        SparkSession.builder
        .appName("ETLPipeline")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    # Output directory
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "output"
    )
    
    # ===========================================
    # PHASE 1: EXTRACT
    # ===========================================
    print("="*50)
    print("  PHASE 1: EXTRACT")
    print("="*50 + "\n")
    
    # Simulate raw data sources (in real scenario, read from files/databases)
    
    # Source 1: Orders (messy data)
    print("Extracting orders data...")
    orders_raw = spark.createDataFrame([
        (1, "2023-01-15", "C001", "SHIPPED", 150.00, "  Widget A  "),
        (2, "2023-01-16", "C002", "shipped", 200.00, "Widget B"),
        (3, "2023/01/17", "C001", "PENDING", None, "widget c"),  # Bad date, null amount
        (4, "2023-01-18", "C003", "SHIPPED", 300.00, "Widget A"),
        (5, "2023-01-19", "C002", "cancelled", 100.00, "Widget B"),
        (6, "2023-01-20", "C004", "SHIPPED", 250.00, "WIDGET D"),
        (7, "2023-01-21", "C001", "PENDING", 175.00, "Widget A"),
        (8, "2023-01-22", "C005", "shipped", 400.00, "Widget E"),
        (9, "2023-01-23", "C003", "SHIPPED", 125.00, "Widget B"),
        (10, "2023-01-24", "", "shipped", 225.00, "Widget A"),  # Missing customer
    ], ["order_id", "order_date", "customer_id", "status", "amount", "product"])
    
    print(f"  ✓ Orders: {orders_raw.count()} records")
    
    # Source 2: Customers
    print("Extracting customers data...")
    customers_raw = spark.createDataFrame([
        ("C001", "Alice Smith", "alice@email.com", "Gold"),
        ("C002", "Bob Jones", "bob@email.com", "Silver"),
        ("C003", "Charlie Brown", "charlie@email.com", "Bronze"),
        ("C004", "Diana Lee", "diana@email.com", "Gold"),
        ("C005", "Eve Wilson", "eve@email.com", "Silver")
    ], ["customer_id", "name", "email", "tier"])
    
    print(f"  ✓ Customers: {customers_raw.count()} records")
    
    # Source 3: Products  
    print("Extracting products data...")
    products_raw = spark.createDataFrame([
        ("Widget A", "Electronics", 25.00),
        ("Widget B", "Electronics", 35.00),
        ("Widget C", "Electronics", 45.00),
        ("Widget D", "Clothing", 50.00),
        ("Widget E", "Clothing", 75.00)
    ], ["product_name", "category", "unit_price"])
    
    print(f"  ✓ Products: {products_raw.count()} records")
    print()
    
    # ===========================================
    # PHASE 2: TRANSFORM
    # ===========================================
    print("="*50)
    print("  PHASE 2: TRANSFORM")
    print("="*50 + "\n")
    
    # -----------------------------------------
    # Step 2.1: Clean Orders
    # -----------------------------------------
    print("--- Step 2.1: Clean Orders ---\n")
    
    print("Original orders schema:")
    orders_raw.printSchema()
    
    print("Sample raw data:")
    orders_raw.show(5, truncate=False)
    
    # Clean orders
    orders_cleaned = (
        orders_raw
        # Standardize date format
        .withColumn(
            "order_date",
            to_date(
                regexp_replace(col("order_date"), "/", "-"),
                "yyyy-MM-dd"
            )
        )
        # Clean product name
        .withColumn("product", upper(trim(col("product"))))
        # Standardize status
        .withColumn("status", upper(col("status")))
        # Handle null amounts
        .withColumn("amount", coalesce(col("amount"), lit(0.0)))
        # Handle empty customer IDs
        .withColumn(
            "customer_id",
            when(col("customer_id") == "", None).otherwise(col("customer_id"))
        )
        # Filter out invalid records
        .filter(col("customer_id").isNotNull())
        # Add audit column
        .withColumn("processed_at", current_timestamp())
    )
    
    print("Cleaned orders:")
    orders_cleaned.show()
    print(f"  ✓ After cleaning: {orders_cleaned.count()} records (removed {orders_raw.count() - orders_cleaned.count()} invalid)")
    print()
    
    # -----------------------------------------
    # Step 2.2: Data Quality Checks
    # -----------------------------------------
    print("--- Step 2.2: Data Quality Checks ---\n")
    
    # Check for duplicates
    total_orders = orders_cleaned.count()
    distinct_orders = orders_cleaned.select("order_id").distinct().count()
    duplicates = total_orders - distinct_orders
    print(f"  Duplicate check: {duplicates} duplicates found")
    
    # Check for null values
    print("  Null value counts:")
    for column in orders_cleaned.columns:
        null_count = orders_cleaned.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"    - {column}: {null_count} nulls")
    
    # Validate referential integrity
    orders_customer_ids = orders_cleaned.select("customer_id").distinct()
    missing_customers = orders_customer_ids.join(
        customers_raw, 
        orders_customer_ids.customer_id == customers_raw.customer_id,
        "left_anti"
    ).count()
    print(f"  Missing customers: {missing_customers}")
    print()
    
    # -----------------------------------------
    # Step 2.3: Enrich Data
    # -----------------------------------------
    print("--- Step 2.3: Enrich Data ---\n")
    
    # Add date dimensions
    orders_enriched = (
        orders_cleaned
        .withColumn("order_year", year(col("order_date")))
        .withColumn("order_month", month(col("order_date")))
        .withColumn("order_quarter", quarter(col("order_date")))
        .withColumn("day_of_week", dayofweek(col("order_date")))
        .withColumn(
            "is_weekend",
            when(col("day_of_week").isin(1, 7), True).otherwise(False)
        )
    )
    
    # Join with customers (broadcast small table)
    orders_with_customer = orders_enriched.join(
        broadcast(customers_raw),
        "customer_id",
        "left"
    )
    
    # Join with products (broadcast and fuzzy match on product name)
    orders_full = orders_with_customer.join(
        broadcast(products_raw),
        upper(trim(orders_with_customer.product)) == upper(trim(products_raw.product_name)),
        "left"
    ).select(
        orders_with_customer["*"],
        products_raw.category,
        products_raw.unit_price
    )
    
    print("Enriched orders:")
    orders_full.select(
        "order_id", "order_date", "customer_id", "name", "tier",
        "product", "category", "status", "amount"
    ).show()
    
    print(f"  ✓ After enrichment: {orders_full.count()} records with {len(orders_full.columns)} columns")
    print()
    
    # -----------------------------------------
    # Step 2.4: Aggregate Data
    # -----------------------------------------
    print("--- Step 2.4: Aggregate Data ---\n")
    
    # Filter to only shipped orders for analysis
    shipped_orders = orders_full.filter(col("status") == "SHIPPED")
    
    # Aggregate 1: Daily sales summary
    daily_sales = (
        shipped_orders
        .groupBy("order_date", "category")
        .agg(
            count("*").alias("order_count"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_round(spark_sum("amount"), 2).alias("total_revenue"),
            spark_round(avg("amount"), 2).alias("avg_order_value")
        )
        .orderBy("order_date", "category")
    )
    
    print("Daily Sales Summary:")
    daily_sales.show()
    
    # Aggregate 2: Customer summary
    customer_summary = (
        shipped_orders
        .groupBy("customer_id", "name", "tier")
        .agg(
            count("*").alias("total_orders"),
            spark_round(spark_sum("amount"), 2).alias("total_spent"),
            spark_round(avg("amount"), 2).alias("avg_order_value"),
            spark_round(
                spark_sum(when(col("category") == "Electronics", col("amount")).otherwise(0)),
                2
            ).alias("electronics_spent")
        )
        .orderBy(col("total_spent").desc())
    )
    
    print("Customer Summary:")
    customer_summary.show()
    
    # Aggregate 3: Product performance
    product_performance = (
        shipped_orders
        .groupBy("product", "category")
        .agg(
            count("*").alias("times_ordered"),
            spark_round(spark_sum("amount"), 2).alias("total_revenue")
        )
        .withColumn(
            "rank",
            row_number().over(Window.orderBy(col("total_revenue").desc()))
        )
        .orderBy("rank")
    )
    
    print("Product Performance:")
    product_performance.show()
    print()
    
    # ===========================================
    # PHASE 3: LOAD
    # ===========================================
    print("="*50)
    print("  PHASE 3: LOAD")
    print("="*50 + "\n")
    
    # Create output directories
    os.makedirs(output_dir, exist_ok=True)
    
    # -----------------------------------------
    # Step 3.1: Save as Parquet (columnar, compressed)
    # -----------------------------------------
    print("--- Step 3.1: Save to Parquet ---\n")
    
    parquet_path = os.path.join(output_dir, "orders_processed.parquet")
    if os.path.exists(parquet_path):
        shutil.rmtree(parquet_path)
    
    (
        orders_full
        .select(
            "order_id", "order_date", "customer_id", "name", "email", "tier",
            "product", "category", "status", "amount",
            "order_year", "order_month", "order_quarter"
        )
        .write
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .parquet(parquet_path)
    )
    print(f"  ✓ Saved to: {parquet_path}")
    
    # -----------------------------------------
    # Step 3.2: Save aggregates as CSV
    # -----------------------------------------
    print("--- Step 3.2: Save Aggregates as CSV ---\n")
    
    csv_path = os.path.join(output_dir, "customer_summary.csv")
    if os.path.exists(csv_path):
        shutil.rmtree(csv_path)
    
    (
        customer_summary
        .coalesce(1)  # Single file
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(csv_path)
    )
    print(f"  ✓ Saved to: {csv_path}")
    
    # -----------------------------------------
    # Step 3.3: Save as JSON
    # -----------------------------------------
    print("--- Step 3.3: Save as JSON ---\n")
    
    json_path = os.path.join(output_dir, "daily_sales.json")
    if os.path.exists(json_path):
        shutil.rmtree(json_path)
    
    (
        daily_sales
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(json_path)
    )
    print(f"  ✓ Saved to: {json_path}")
    
    # -----------------------------------------
    # Step 3.4: Verify outputs
    # -----------------------------------------
    print("\n--- Step 3.4: Verify Outputs ---\n")
    
    # Read back parquet
    verify_parquet = spark.read.parquet(parquet_path)
    print(f"  Parquet verification: {verify_parquet.count()} records")
    
    # Show partitions
    print(f"  Partitions: {verify_parquet.select('order_year', 'order_month').distinct().count()}")
    print()
    
    # ===========================================
    # PIPELINE SUMMARY
    # ===========================================
    print("="*50)
    print("  ETL PIPELINE SUMMARY")
    print("="*50 + "\n")
    
    print("Extract:")
    print(f"  - Orders: {orders_raw.count()} records")
    print(f"  - Customers: {customers_raw.count()} records")
    print(f"  - Products: {products_raw.count()} records")
    print()
    print("Transform:")
    print(f"  - Cleaned orders: {orders_cleaned.count()} records")
    print(f"  - Enriched orders: {orders_full.count()} records")
    print(f"  - Shipped orders (for analysis): {shipped_orders.count()} records")
    print()
    print("Load:")
    print(f"  - Parquet: {parquet_path}")
    print(f"  - CSV: {csv_path}")
    print(f"  - JSON: {json_path}")
    print()
    
    # ===========================================
    # CLEANUP
    # ===========================================
    spark.stop()
    print("="*60)
    print("  ETL Pipeline Complete! 🎉")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
