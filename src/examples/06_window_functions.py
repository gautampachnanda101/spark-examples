#!/usr/bin/env python3
"""
Example 06: Window Functions
============================

Master window functions in Spark:
- Ranking functions (row_number, rank, dense_rank)
- Aggregate window functions
- LAG and LEAD
- Running totals and moving averages
- Frame specifications

Run: python src/examples/06_window_functions.py
     OR: make run-window
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, ntile, percent_rank, cume_dist,
    lag, lead, first, last, sum as spark_sum, avg, count, 
    min as spark_min, max as spark_max,
    round as spark_round
)


def main():
    print("\n" + "="*60)
    print("  EXAMPLE 06: Window Functions")
    print("="*60 + "\n")
    
    spark = (
        SparkSession.builder
        .appName("WindowFunctions")
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
    
    # Sales data with dates
    sales = spark.createDataFrame([
        ("2023-01-01", "Electronics", "Widget A", 100, 25.00),
        ("2023-01-02", "Electronics", "Widget B", 150, 35.00),
        ("2023-01-03", "Electronics", "Widget A", 80, 25.00),
        ("2023-01-04", "Electronics", "Widget B", 120, 35.00),
        ("2023-01-05", "Electronics", "Widget A", 90, 25.00),
        ("2023-01-01", "Clothing", "Shirt", 200, 15.00),
        ("2023-01-02", "Clothing", "Pants", 100, 40.00),
        ("2023-01-03", "Clothing", "Shirt", 180, 15.00),
        ("2023-01-04", "Clothing", "Pants", 110, 40.00),
        ("2023-01-05", "Clothing", "Shirt", 220, 15.00)
    ], ["sale_date", "category", "product", "quantity", "unit_price"])
    
    sales = sales.withColumn("revenue", col("quantity") * col("unit_price"))
    sales = sales.withColumn("sale_date", col("sale_date").cast("date"))
    
    print("Sales data:")
    sales.show()
    
    # Employee salaries
    employees = spark.createDataFrame([
        ("Engineering", "Alice", 75000),
        ("Engineering", "Bob", 80000),
        ("Engineering", "Charlie", 75000),
        ("Engineering", "Diana", 90000),
        ("Marketing", "Eve", 65000),
        ("Marketing", "Frank", 70000),
        ("Marketing", "Grace", 65000),
        ("Sales", "Henry", 60000),
        ("Sales", "Ivy", 72000),
        ("Sales", "Jack", 68000)
    ], ["department", "name", "salary"])
    
    print("Employee data:")
    employees.show()
    
    # ===========================================
    # 2. RANKING FUNCTIONS
    # ===========================================
    print("--- Ranking Functions ---\n")
    
    # Define window specification
    dept_window = Window.partitionBy("department").orderBy(col("salary").desc())
    
    # Row number - unique sequential number
    # Rank - same rank for ties, gaps after ties
    # Dense rank - same rank for ties, no gaps
    
    ranked = employees.select(
        "department",
        "name",
        "salary",
        row_number().over(dept_window).alias("row_num"),
        rank().over(dept_window).alias("rank"),
        dense_rank().over(dept_window).alias("dense_rank")
    )
    
    print("Ranking employees by salary within department:")
    ranked.show()
    
    # NTILE - divide into buckets
    print("NTILE - divide into quartiles:")
    employees.select(
        "department",
        "name",
        "salary",
        ntile(4).over(Window.orderBy(col("salary").desc())).alias("quartile")
    ).show()
    
    # Percent rank and cumulative distribution
    print("Percent rank and cumulative distribution:")
    employees.select(
        "name",
        "salary",
        spark_round(percent_rank().over(Window.orderBy("salary")), 2).alias("percent_rank"),
        spark_round(cume_dist().over(Window.orderBy("salary")), 2).alias("cume_dist")
    ).show()
    
    # ===========================================
    # 3. TOP N PER GROUP
    # ===========================================
    print("--- Top N Per Group ---\n")
    
    # Top 2 earners per department
    top_earners = (
        employees
        .withColumn("rank", row_number().over(dept_window))
        .filter(col("rank") <= 2)
        .drop("rank")
    )
    
    print("Top 2 earners per department:")
    top_earners.show()
    
    # ===========================================
    # 4. LAG AND LEAD
    # ===========================================
    print("--- LAG and LEAD ---\n")
    
    # Order sales by date within category
    date_window = Window.partitionBy("category").orderBy("sale_date")
    
    # LAG - previous row value
    # LEAD - next row value
    lag_lead = (
        sales
        .filter(col("product").isin("Widget A", "Shirt"))  # Simplify
        .select(
            "category",
            "sale_date",
            "revenue",
            lag("revenue", 1).over(date_window).alias("prev_revenue"),
            lead("revenue", 1).over(date_window).alias("next_revenue"),
            (col("revenue") - lag("revenue", 1).over(date_window)).alias("change")
        )
    )
    
    print("Previous and next day revenue:")
    lag_lead.orderBy("category", "sale_date").show()
    
    # LAG with default value
    print("LAG with default value of 0:")
    sales.select(
        "category",
        "sale_date",
        "revenue",
        lag("revenue", 1, 0).over(date_window).alias("prev_revenue")
    ).filter(col("product") == "Widget A").show()
    
    # ===========================================
    # 5. RUNNING TOTALS
    # ===========================================
    print("--- Running Totals ---\n")
    
    # Window from start to current row
    running_window = (
        Window
        .partitionBy("category")
        .orderBy("sale_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    running_totals = (
        sales
        .filter(col("product").isin("Widget A", "Shirt"))
        .select(
            "category",
            "sale_date",
            "revenue",
            spark_sum("revenue").over(running_window).alias("running_total"),
            count("*").over(running_window).alias("running_count")
        )
    )
    
    print("Running totals by category:")
    running_totals.orderBy("category", "sale_date").show()
    
    # ===========================================
    # 6. MOVING AVERAGES
    # ===========================================
    print("--- Moving Averages ---\n")
    
    # 3-day moving average (current + 2 preceding)
    moving_window = (
        Window
        .partitionBy("category")
        .orderBy("sale_date")
        .rowsBetween(-2, Window.currentRow)  # 2 rows before to current
    )
    
    moving_avg = (
        sales
        .filter(col("product").isin("Widget A", "Shirt"))
        .select(
            "category",
            "sale_date",
            "revenue",
            spark_round(avg("revenue").over(moving_window), 2).alias("moving_avg_3day")
        )
    )
    
    print("3-day moving average:")
    moving_avg.orderBy("category", "sale_date").show()
    
    # ===========================================
    # 7. AGGREGATE WINDOW FUNCTIONS
    # ===========================================
    print("--- Aggregate Window Functions ---\n")
    
    # Aggregates over the entire partition
    full_window = Window.partitionBy("department")
    
    emp_with_stats = employees.select(
        "department",
        "name",
        "salary",
        spark_sum("salary").over(full_window).alias("dept_total"),
        spark_round(avg("salary").over(full_window), 0).alias("dept_avg"),
        spark_min("salary").over(full_window).alias("dept_min"),
        spark_max("salary").over(full_window).alias("dept_max"),
        spark_round(
            col("salary") * 100 / spark_sum("salary").over(full_window), 
            1
        ).alias("pct_of_dept")
    )
    
    print("Employee salaries with department statistics:")
    emp_with_stats.show()
    
    # Compare to group average
    print("Variance from department average:")
    employees.select(
        "department",
        "name",
        "salary",
        spark_round(avg("salary").over(full_window), 0).alias("dept_avg"),
        (col("salary") - avg("salary").over(full_window)).alias("vs_avg")
    ).orderBy("department", col("vs_avg").desc()).show()
    
    # ===========================================
    # 8. FIRST AND LAST VALUES
    # ===========================================
    print("--- First and Last Values ---\n")
    
    ordered_window = (
        Window
        .partitionBy("category")
        .orderBy("sale_date")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    
    first_last = (
        sales
        .filter(col("product").isin("Widget A", "Shirt"))
        .select(
            "category",
            "sale_date",
            "revenue",
            first("revenue").over(ordered_window).alias("first_revenue"),
            last("revenue").over(ordered_window).alias("last_revenue")
        )
    )
    
    print("First and last revenue per category:")
    first_last.orderBy("category", "sale_date").show()
    
    # ===========================================
    # 9. FRAME SPECIFICATIONS
    # ===========================================
    print("--- Frame Specifications ---\n")
    print("""
Window Frame Types:
- rowsBetween(start, end): Physical rows
- rangeBetween(start, end): Logical range based on ORDER BY value

Frame Boundaries:
- Window.unboundedPreceding: Start of partition
- Window.unboundedFollowing: End of partition  
- Window.currentRow: Current row
- N: N rows before (negative) or after (positive)
""")
    
    # Example: Sum of surrounding rows
    surrounding_window = (
        Window
        .partitionBy("category")
        .orderBy("sale_date")
        .rowsBetween(-1, 1)  # 1 before, current, 1 after
    )
    
    surrounding_sum = (
        sales
        .filter(col("product").isin("Widget A", "Shirt"))
        .select(
            "category",
            "sale_date",
            "revenue",
            spark_sum("revenue").over(surrounding_window).alias("surrounding_sum")
        )
    )
    
    print("Sum of current and surrounding rows:")
    surrounding_sum.orderBy("category", "sale_date").show()
    
    # ===========================================
    # CLEANUP
    # ===========================================
    spark.stop()
    print("\n" + "="*60)
    print("  Window Functions Complete! 🎉")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
