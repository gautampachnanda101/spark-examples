#!/usr/bin/env python3
"""
Example 05: Joins
=================

Master join operations in Spark:
- Inner, Left, Right, Full Outer joins
- Cross joins and Self joins
- Broadcast joins (optimization)
- Handling duplicate column names

Run: python src/examples/05_joins.py
     OR: make run-joins
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast


def main():
    print("\n" + "="*60)
    print("  EXAMPLE 05: Joins")
    print("="*60 + "\n")
    
    spark = (
        SparkSession.builder
        .appName("Joins")
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
    
    # Employees
    employees = spark.createDataFrame([
        (1, "Alice", 101),
        (2, "Bob", 102),
        (3, "Charlie", 101),
        (4, "Diana", 103),
        (5, "Eve", None)  # No department
    ], ["emp_id", "name", "dept_id"])
    
    # Departments
    departments = spark.createDataFrame([
        (101, "Engineering", "Building A"),
        (102, "Marketing", "Building B"),
        (103, "Sales", "Building C"),
        (104, "HR", "Building D")  # No employees
    ], ["dept_id", "dept_name", "location"])
    
    # Salaries
    salaries = spark.createDataFrame([
        (1, 75000, 2023),
        (2, 65000, 2023),
        (3, 80000, 2023),
        (4, 70000, 2023),
        (1, 70000, 2022),
        (2, 62000, 2022)
    ], ["emp_id", "salary", "year"])
    
    # Projects
    projects = spark.createDataFrame([
        ("P1", "Project Alpha", 1),
        ("P2", "Project Beta", 1),
        ("P3", "Project Gamma", 2),
        ("P4", "Project Delta", 3)
    ], ["project_id", "project_name", "lead_emp_id"])
    
    print("Employees:")
    employees.show()
    print("Departments:")
    departments.show()
    print("Salaries:")
    salaries.show()
    print("Projects:")
    projects.show()
    
    # ===========================================
    # 2. INNER JOIN
    # ===========================================
    print("--- Inner Join ---\n")
    print("Only matching rows from both tables\n")
    
    # Simple inner join
    inner_join = employees.join(
        departments,
        employees.dept_id == departments.dept_id,
        "inner"
    ).select(
        employees.emp_id,
        employees.name,
        departments.dept_name,
        departments.location
    )
    
    print("Employees with their departments:")
    inner_join.show()
    
    # Alternative syntax using column name (when same name)
    alt_join = employees.join(departments, "dept_id", "inner")
    print("Alternative syntax (join on same column name):")
    alt_join.show()
    
    # ===========================================
    # 3. LEFT JOIN
    # ===========================================
    print("--- Left (Outer) Join ---\n")
    print("All rows from left table, matching from right\n")
    
    left_join = employees.join(
        departments,
        employees.dept_id == departments.dept_id,
        "left"
    ).select(
        employees.emp_id,
        employees.name,
        departments.dept_name
    )
    
    print("All employees (even without department):")
    left_join.show()
    
    # ===========================================
    # 4. RIGHT JOIN
    # ===========================================
    print("--- Right (Outer) Join ---\n")
    print("All rows from right table, matching from left\n")
    
    right_join = employees.join(
        departments,
        employees.dept_id == departments.dept_id,
        "right"
    ).select(
        departments.dept_id,
        departments.dept_name,
        employees.name
    )
    
    print("All departments (even without employees):")
    right_join.show()
    
    # ===========================================
    # 5. FULL OUTER JOIN
    # ===========================================
    print("--- Full Outer Join ---\n")
    print("All rows from both tables\n")
    
    full_join = employees.join(
        departments,
        employees.dept_id == departments.dept_id,
        "full"
    ).select(
        employees.emp_id,
        employees.name,
        departments.dept_name
    )
    
    print("All employees and all departments:")
    full_join.show()
    
    # ===========================================
    # 6. CROSS JOIN
    # ===========================================
    print("--- Cross Join (Cartesian Product) ---\n")
    print("Every combination of rows from both tables\n")
    
    # Small datasets for cross join demo
    colors = spark.createDataFrame([
        ("Red",), ("Blue",), ("Green",)
    ], ["color"])
    
    sizes = spark.createDataFrame([
        ("S",), ("M",), ("L",)
    ], ["size"])
    
    cross_join = colors.crossJoin(sizes)
    print("All color-size combinations:")
    cross_join.show()
    
    # ===========================================
    # 7. SELF JOIN
    # ===========================================
    print("--- Self Join ---\n")
    print("Join table with itself\n")
    
    # Employee hierarchy
    emp_hierarchy = spark.createDataFrame([
        (1, "Alice", None),     # CEO
        (2, "Bob", 1),          # Reports to Alice
        (3, "Charlie", 1),      # Reports to Alice
        (4, "Diana", 2),        # Reports to Bob
        (5, "Eve", 2)           # Reports to Bob
    ], ["emp_id", "name", "manager_id"])
    
    # Self join to get manager names
    emp_with_mgr = emp_hierarchy.alias("e").join(
        emp_hierarchy.alias("m"),
        col("e.manager_id") == col("m.emp_id"),
        "left"
    ).select(
        col("e.emp_id"),
        col("e.name").alias("employee"),
        col("m.name").alias("manager")
    )
    
    print("Employees with their managers:")
    emp_with_mgr.show()
    
    # ===========================================
    # 8. MULTIPLE JOINS
    # ===========================================
    print("--- Multiple Joins ---\n")
    
    # Join employees -> departments -> add salaries
    multi_join = (
        employees
        .join(departments, employees.dept_id == departments.dept_id, "left")
        .join(
            salaries.filter(col("year") == 2023),
            employees.emp_id == salaries.emp_id,
            "left"
        )
        .select(
            employees.name,
            departments.dept_name,
            salaries.salary
        )
    )
    
    print("Employees with department and salary:")
    multi_join.show()
    
    # ===========================================
    # 9. BROADCAST JOIN (Optimization)
    # ===========================================
    print("--- Broadcast Join ---\n")
    print("Efficient when one table is small\n")
    
    # Broadcast the smaller table
    broadcast_join = employees.join(
        broadcast(departments),  # Broadcast small table to all workers
        employees.dept_id == departments.dept_id,
        "left"
    ).select(
        employees.name,
        departments.dept_name
    )
    
    print("Broadcast join result (same as regular, but faster):")
    broadcast_join.show()
    
    # Explain the plan to see broadcast
    print("Query Plan (notice BroadcastHashJoin):")
    broadcast_join.explain()
    
    # ===========================================
    # 10. HANDLING DUPLICATE COLUMN NAMES
    # ===========================================
    print("\n--- Handling Duplicate Columns ---\n")
    
    # Tables with same column names
    df1 = spark.createDataFrame([
        (1, "A", 100),
        (2, "B", 200)
    ], ["id", "name", "value"])
    
    df2 = spark.createDataFrame([
        (1, "X", 1000),
        (2, "Y", 2000)
    ], ["id", "name", "value"])  # Same column names!
    
    # Method 1: Rename before join
    renamed_join = df1.join(
        df2.withColumnRenamed("name", "name2").withColumnRenamed("value", "value2"),
        "id"
    )
    print("Method 1 - Rename before join:")
    renamed_join.show()
    
    # Method 2: Use aliases
    aliased_join = df1.alias("a").join(
        df2.alias("b"),
        col("a.id") == col("b.id")
    ).select(
        col("a.id"),
        col("a.name").alias("name_a"),
        col("b.name").alias("name_b"),
        col("a.value").alias("value_a"),
        col("b.value").alias("value_b")
    )
    print("Method 2 - Use aliases:")
    aliased_join.show()
    
    # ===========================================
    # 11. SEMI AND ANTI JOINS
    # ===========================================
    print("--- Semi and Anti Joins ---\n")
    
    # Left Semi Join - only left rows that have match (like EXISTS)
    print("Left Semi Join (employees with departments):")
    semi_join = employees.join(departments, "dept_id", "left_semi")
    semi_join.show()
    
    # Left Anti Join - only left rows that don't have match (like NOT EXISTS)
    print("Left Anti Join (employees without departments):")
    anti_join = employees.join(departments, "dept_id", "left_anti")
    anti_join.show()
    
    # ===========================================
    # CLEANUP
    # ===========================================
    spark.stop()
    print("\n" + "="*60)
    print("  Joins Complete! 🎉")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
