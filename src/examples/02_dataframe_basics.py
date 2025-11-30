#!/usr/bin/env python3
"""
Example 02: DataFrame Basics
============================

Deep dive into DataFrame operations:
- Creating DataFrames from various sources
- Column operations and expressions
- Filtering and transformations
- Working with nulls and data types

Run: python src/examples/02_dataframe_basics.py
     OR: make run-basics
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, 
    upper, lower, concat, substring, trim,
    length, regexp_replace, split,
    year, month, dayofmonth, current_date, datediff,
    round as spark_round, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, BooleanType, ArrayType
)
from datetime import date


def main():
    print("\n" + "="*60)
    print("  EXAMPLE 02: DataFrame Basics")
    print("="*60 + "\n")
    
    spark = (
        SparkSession.builder
        .appName("DataFrameBasics")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    # ===========================================
    # 1. CREATING DATAFRAMES
    # ===========================================
    print("--- Creating DataFrames ---\n")
    
    # Method 1: From Python list with schema inference
    data1 = [(1, "Alice", 50000.0), (2, "Bob", 60000.0)]
    df1 = spark.createDataFrame(data1, ["id", "name", "salary"])
    print("From list with column names:")
    df1.show()
    
    # Method 2: With explicit schema
    schema = StructType([
        StructField("id", IntegerType(), False),  # Not nullable
        StructField("name", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("start_date", DateType(), True)
    ])
    
    data2 = [
        (1, "Alice", 50000.0, date(2020, 1, 15)),
        (2, "Bob", 60000.0, date(2019, 6, 1)),
        (3, "Charlie", None, date(2021, 3, 10)),
        (4, None, 55000.0, None)
    ]
    
    df2 = spark.createDataFrame(data2, schema)
    print("With explicit schema:")
    df2.show()
    df2.printSchema()
    
    # Method 3: From dictionary list (Row objects)
    from pyspark.sql import Row
    data3 = [
        Row(id=1, name="Alice", active=True),
        Row(id=2, name="Bob", active=False)
    ]
    df3 = spark.createDataFrame(data3)
    print("From Row objects:")
    df3.show()
    
    # ===========================================
    # 2. COLUMN OPERATIONS
    # ===========================================
    print("--- Column Operations ---\n")
    
    # Create sample data
    employees = spark.createDataFrame([
        (1, "Alice Smith", "Engineering", 75000, "2020-01-15"),
        (2, "Bob Jones", "Marketing", 65000, "2019-06-01"),
        (3, "Charlie Brown", "Engineering", 80000, "2018-03-20"),
        (4, "Diana Lee", "Sales", 70000, "2021-09-10"),
        (5, "Eve Wilson", "Engineering", 72000, "2022-02-28")
    ], ["id", "full_name", "department", "salary", "hire_date"])
    
    employees = employees.withColumn("hire_date", col("hire_date").cast("date"))
    
    print("Original data:")
    employees.show()
    
    # Different ways to reference columns
    print("Ways to select columns:")
    
    # Using string
    employees.select("full_name", "salary").show(3)
    
    # Using col() function
    employees.select(col("full_name"), col("salary")).show(3)
    
    # Using DataFrame attribute
    employees.select(employees.full_name, employees.salary).show(3)
    
    # ===========================================
    # 3. ADDING AND MODIFYING COLUMNS
    # ===========================================
    print("--- Adding/Modifying Columns ---\n")
    
    # Add literal column
    df_with_country = employees.withColumn("country", lit("USA"))
    print("Add literal column:")
    df_with_country.show(3)
    
    # Add computed column
    df_with_bonus = employees.withColumn("bonus", col("salary") * 0.10)
    print("Add computed column (10% bonus):")
    df_with_bonus.show()
    
    # Rename column
    df_renamed = employees.withColumnRenamed("full_name", "employee_name")
    print("Renamed column:")
    df_renamed.columns
    print(f"Columns: {df_renamed.columns}")
    print()
    
    # Multiple transformations chained
    df_transformed = (
        employees
        .withColumn("annual_bonus", col("salary") * 0.10)
        .withColumn("total_comp", col("salary") + col("salary") * 0.10)
        .withColumnRenamed("full_name", "name")
        .drop("id")
    )
    print("Multiple transformations:")
    df_transformed.show()
    
    # ===========================================
    # 4. STRING FUNCTIONS
    # ===========================================
    print("--- String Functions ---\n")
    
    names_df = spark.createDataFrame([
        ("  Alice Smith  ",),
        ("BOB JONES",),
        ("charlie brown",),
        ("Diana-Lee",)
    ], ["name"])
    
    string_ops = (
        names_df
        .withColumn("trimmed", trim(col("name")))
        .withColumn("upper", upper(trim(col("name"))))
        .withColumn("lower", lower(trim(col("name"))))
        .withColumn("length", length(trim(col("name"))))
        .withColumn("first_name", split(trim(col("name")), " |\\-")[0])
    )
    
    print("String operations:")
    string_ops.show(truncate=False)
    
    # ===========================================
    # 5. CONDITIONAL EXPRESSIONS
    # ===========================================
    print("--- Conditional Expressions ---\n")
    
    # when/otherwise (like CASE WHEN in SQL)
    df_with_level = employees.withColumn(
        "level",
        when(col("salary") >= 80000, "Senior")
        .when(col("salary") >= 70000, "Mid")
        .otherwise("Junior")
    )
    print("Salary levels using when/otherwise:")
    df_with_level.show()
    
    # Multiple conditions
    df_with_flags = employees.withColumn(
        "is_senior_engineer",
        (col("department") == "Engineering") & (col("salary") >= 75000)
    )
    print("Boolean condition:")
    df_with_flags.show()
    
    # ===========================================
    # 6. HANDLING NULL VALUES
    # ===========================================
    print("--- Handling Nulls ---\n")
    
    # Data with nulls
    null_data = spark.createDataFrame([
        (1, "Alice", 50000),
        (2, None, 60000),
        (3, "Charlie", None),
        (4, None, None)
    ], ["id", "name", "salary"])
    
    print("Original data with nulls:")
    null_data.show()
    
    # Drop rows with any nulls
    print("Drop rows with any nulls:")
    null_data.dropna().show()
    
    # Drop rows with nulls in specific columns
    print("Drop rows with null names:")
    null_data.dropna(subset=["name"]).show()
    
    # Fill nulls with default values
    print("Fill nulls:")
    null_data.fillna({"name": "Unknown", "salary": 0}).show()
    
    # Using coalesce
    print("Using coalesce:")
    null_data.withColumn(
        "name_filled",
        coalesce(col("name"), lit("No Name"))
    ).show()
    
    # ===========================================
    # 7. DATE OPERATIONS
    # ===========================================
    print("--- Date Operations ---\n")
    
    today = current_date()
    
    date_ops = (
        employees
        .withColumn("hire_year", year(col("hire_date")))
        .withColumn("hire_month", month(col("hire_date")))
        .withColumn("tenure_days", datediff(today, col("hire_date")))
        .withColumn("tenure_years", spark_round(datediff(today, col("hire_date")) / 365, 1))
    )
    
    print("Date operations:")
    date_ops.select("full_name", "hire_date", "hire_year", "tenure_days", "tenure_years").show()
    
    # ===========================================
    # 8. TYPE CASTING
    # ===========================================
    print("--- Type Casting ---\n")
    
    mixed_df = spark.createDataFrame([
        ("1", "100.5", "2023-06-15"),
        ("2", "200.75", "2023-07-20")
    ], ["id_str", "amount_str", "date_str"])
    
    print("Original string columns:")
    mixed_df.printSchema()
    
    casted_df = (
        mixed_df
        .withColumn("id", col("id_str").cast(IntegerType()))
        .withColumn("amount", col("amount_str").cast(DoubleType()))
        .withColumn("date", col("date_str").cast(DateType()))
        .drop("id_str", "amount_str", "date_str")
    )
    
    print("After casting:")
    casted_df.printSchema()
    casted_df.show()
    
    # ===========================================
    # 9. DISTINCT AND DUPLICATES
    # ===========================================
    print("--- Distinct and Duplicates ---\n")
    
    dup_data = spark.createDataFrame([
        ("Alice", "Engineering"),
        ("Bob", "Marketing"),
        ("Alice", "Engineering"),  # Duplicate
        ("Charlie", "Engineering"),
        ("Bob", "Marketing")  # Duplicate
    ], ["name", "department"])
    
    print(f"Original count: {dup_data.count()}")
    print(f"Distinct count: {dup_data.distinct().count()}")
    
    print("\nDistinct rows:")
    dup_data.distinct().show()
    
    print("Drop duplicates by department:")
    dup_data.dropDuplicates(["department"]).show()
    
    # ===========================================
    # CLEANUP
    # ===========================================
    spark.stop()
    print("\n" + "="*60)
    print("  DataFrame Basics Complete! 🎉")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
