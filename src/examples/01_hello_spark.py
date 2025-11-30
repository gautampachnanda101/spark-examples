#!/usr/bin/env python3
"""
Example 01: Hello Spark
=======================

This is your first Spark program! Learn how to:
- Create a SparkSession
- Create a simple DataFrame
- Perform basic operations
- Display results

Run: python src/examples/01_hello_spark.py
     OR: make run-hello
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def main():
    # ===========================================
    # 1. CREATE SPARKSESSION
    # ===========================================
    print("\n" + "="*60)
    print("  EXAMPLE 01: Hello Spark!")
    print("="*60 + "\n")
    
    # SparkSession is the entry point to all Spark functionality
    spark = (
        SparkSession.builder
        .appName("HelloSpark")
        .master("local[*]")  # Use all available CPU cores
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    
    # Reduce log verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✓ SparkSession created")
    print(f"  Spark Version: {spark.version}")
    print(f"  App Name: {spark.sparkContext.appName}")
    print()
    
    # ===========================================
    # 2. CREATE DATA MANUALLY
    # ===========================================
    print("--- Creating Data ---\n")
    
    # Method 1: From a list of tuples
    data = [
        ("Alice", 28, "Engineering"),
        ("Bob", 35, "Marketing"),
        ("Charlie", 42, "Engineering"),
        ("Diana", 31, "Sales"),
        ("Eve", 26, "Engineering")
    ]
    
    columns = ["name", "age", "department"]
    
    # Create DataFrame from data
    df = spark.createDataFrame(data, columns)
    
    print("✓ DataFrame created from Python list")
    print()
    
    # ===========================================
    # 3. VIEW THE DATA
    # ===========================================
    print("--- Viewing Data ---\n")
    
    # Show the data (default: 20 rows)
    print("df.show():")
    df.show()
    
    # Show specific number of rows
    print("df.show(3):")
    df.show(3)
    
    # Show without truncation
    print("df.show(truncate=False):")
    df.show(truncate=False)
    
    # ===========================================
    # 4. INSPECT THE SCHEMA
    # ===========================================
    print("--- Schema Inspection ---\n")
    
    # Print schema
    print("df.printSchema():")
    df.printSchema()
    
    # Get column names
    print(f"Columns: {df.columns}")
    
    # Get data types
    print(f"Data Types: {df.dtypes}")
    
    # Count rows
    print(f"Row Count: {df.count()}")
    
    print()
    
    # ===========================================
    # 5. SIMPLE TRANSFORMATIONS
    # ===========================================
    print("--- Simple Transformations ---\n")
    
    # Select specific columns
    print("Select name and department:")
    df.select("name", "department").show()
    
    # Filter rows
    print("Filter: Engineers only:")
    df.filter(df.department == "Engineering").show()
    
    # Add a new column
    print("Add age_next_year column:")
    df.withColumn("age_next_year", df.age + 1).show()
    
    # Sort by age
    print("Sort by age (descending):")
    df.orderBy(df.age.desc()).show()
    
    # ===========================================
    # 6. SIMPLE AGGREGATIONS
    # ===========================================
    print("--- Simple Aggregations ---\n")
    
    # Count by department
    print("Count by department:")
    df.groupBy("department").count().show()
    
    # Average age by department
    print("Average age by department:")
    df.groupBy("department").avg("age").show()
    
    # ===========================================
    # 7. USING SQL
    # ===========================================
    print("--- Using Spark SQL ---\n")
    
    # Register as temp view
    df.createOrReplaceTempView("employees")
    
    # Run SQL query
    result = spark.sql("""
        SELECT 
            department,
            COUNT(*) as employee_count,
            AVG(age) as avg_age
        FROM employees
        GROUP BY department
        ORDER BY employee_count DESC
    """)
    
    print("SQL Query Results:")
    result.show()
    
    # ===========================================
    # 8. COLLECT RESULTS TO PYTHON
    # ===========================================
    print("--- Collecting Results ---\n")
    
    # collect() returns all data to driver (use with caution!)
    rows = df.filter(df.age > 30).collect()
    print(f"Employees over 30: {len(rows)}")
    for row in rows:
        print(f"  - {row.name}: {row.age} years old")
    
    print()
    
    # ===========================================
    # 9. CLEANUP
    # ===========================================
    spark.stop()
    print("✓ SparkSession stopped")
    print("\n" + "="*60)
    print("  Hello Spark Complete! 🎉")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
