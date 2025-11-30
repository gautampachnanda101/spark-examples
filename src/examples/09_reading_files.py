#!/usr/bin/env python3
"""
Example: Reading Files
======================

Learn to read data from various file formats:
- CSV files
- JSON files
- Parquet files

Run: python src/examples/09_reading_files.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType


def main():
    print("\n" + "="*60)
    print("  EXAMPLE 09: Reading Files")
    print("="*60 + "\n")
    
    spark = (
        SparkSession.builder
        .appName("ReadingFiles")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    # Get data directory
    data_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "data"
    )
    
    # ===========================================
    # 1. READING CSV FILES
    # ===========================================
    print("--- Reading CSV Files ---\n")
    
    # Basic CSV read with header
    employees_csv = os.path.join(data_dir, "employees.csv")
    
    print("Basic CSV read:")
    df_basic = spark.read.csv(employees_csv, header=True, inferSchema=True)
    df_basic.show()
    df_basic.printSchema()
    
    # CSV with explicit options
    print("CSV with options:")
    df_options = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("nullValue", "NA")
        .option("dateFormat", "yyyy-MM-dd")
        .csv(employees_csv)
    )
    df_options.show()
    
    # CSV with explicit schema (better performance)
    print("CSV with explicit schema:")
    employee_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("hire_date", DateType(), True)
    ])
    
    df_schema = spark.read.schema(employee_schema).option("header", "true").csv(employees_csv)
    df_schema.printSchema()
    df_schema.show()
    
    # ===========================================
    # 2. READING ORDERS CSV
    # ===========================================
    print("--- Reading Orders CSV ---\n")
    
    orders_csv = os.path.join(data_dir, "orders.csv")
    orders = spark.read.csv(orders_csv, header=True, inferSchema=True)
    
    print("Orders data:")
    orders.show()
    print(f"Total orders: {orders.count()}")
    
    # Simple analysis
    print("\nOrders by status:")
    orders.groupBy("status").count().show()
    
    # ===========================================
    # 3. READING CUSTOMERS CSV
    # ===========================================
    print("--- Reading Customers CSV ---\n")
    
    customers_csv = os.path.join(data_dir, "customers.csv")
    customers = spark.read.csv(customers_csv, header=True, inferSchema=True)
    
    print("Customers data:")
    customers.show()
    
    # ===========================================
    # 4. JOIN EXAMPLE
    # ===========================================
    print("--- Join: Orders with Customers ---\n")
    
    joined = (
        orders
        .join(customers, "customer_id", "left")
        .select(
            orders.order_id,
            orders.order_date,
            customers.name.alias("customer_name"),
            customers.tier,
            orders.product,
            orders.quantity,
            orders.status
        )
    )
    
    print("Orders with customer info:")
    joined.show()
    
    # ===========================================
    # 5. SAVE AS PARQUET
    # ===========================================
    print("--- Save as Parquet ---\n")
    
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "output"
    )
    os.makedirs(output_dir, exist_ok=True)
    
    parquet_path = os.path.join(output_dir, "orders_with_customers.parquet")
    
    import shutil
    if os.path.exists(parquet_path):
        shutil.rmtree(parquet_path)
    
    joined.write.mode("overwrite").parquet(parquet_path)
    print(f"Saved to: {parquet_path}")
    
    # Read back
    print("\nRead back from Parquet:")
    df_parquet = spark.read.parquet(parquet_path)
    df_parquet.show(5)
    
    # ===========================================
    # CLEANUP
    # ===========================================
    spark.stop()
    print("\n" + "="*60)
    print("  Reading Files Complete! 🎉")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
