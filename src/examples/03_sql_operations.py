#!/usr/bin/env python3
"""
Example 03: SQL Operations
==========================

Learn Spark SQL capabilities:
- Creating temporary views
- Running SQL queries
- Joins using SQL
- Subqueries and CTEs
- SQL functions

Run: python src/examples/03_sql_operations.py
     OR: make run-sql
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    print("\n" + "="*60)
    print("  EXAMPLE 03: SQL Operations")
    print("="*60 + "\n")
    
    spark = (
        SparkSession.builder
        .appName("SQLOperations")
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
    
    # Employees table
    employees = spark.createDataFrame([
        (1, "Alice", "Engineering", 75000, 1),
        (2, "Bob", "Marketing", 65000, 2),
        (3, "Charlie", "Engineering", 80000, 1),
        (4, "Diana", "Sales", 70000, 3),
        (5, "Eve", "Engineering", 72000, 1),
        (6, "Frank", "Marketing", 68000, 2),
        (7, "Grace", "Sales", 62000, 3),
        (8, "Henry", "Engineering", 90000, 1)
    ], ["emp_id", "name", "department", "salary", "manager_id"])
    
    # Departments table
    departments = spark.createDataFrame([
        ("Engineering", "Tech Building", 500000),
        ("Marketing", "Marketing Plaza", 300000),
        ("Sales", "Sales Tower", 400000),
        ("HR", "Admin Building", 200000)
    ], ["dept_name", "location", "budget"])
    
    # Managers table
    managers = spark.createDataFrame([
        (1, "Alice", "Engineering Lead"),
        (2, "Bob", "Marketing Lead"),
        (3, "Diana", "Sales Lead")
    ], ["manager_id", "manager_name", "title"])
    
    # Register as temporary views
    employees.createOrReplaceTempView("employees")
    departments.createOrReplaceTempView("departments")
    managers.createOrReplaceTempView("managers")
    
    print("Tables created: employees, departments, managers")
    print("\nemployees:")
    employees.show()
    print("departments:")
    departments.show()
    print("managers:")
    managers.show()
    
    # ===========================================
    # 2. BASIC SELECT QUERIES
    # ===========================================
    print("--- Basic SELECT Queries ---\n")
    
    # Simple SELECT
    print("All employees:")
    spark.sql("SELECT * FROM employees").show()
    
    # SELECT with columns
    print("Names and salaries:")
    spark.sql("SELECT name, salary FROM employees").show()
    
    # WHERE clause
    print("Engineers only:")
    spark.sql("""
        SELECT name, department, salary 
        FROM employees 
        WHERE department = 'Engineering'
    """).show()
    
    # Multiple conditions
    print("Engineers with salary > 75000:")
    spark.sql("""
        SELECT name, department, salary 
        FROM employees 
        WHERE department = 'Engineering' 
          AND salary > 75000
    """).show()
    
    # ===========================================
    # 3. AGGREGATIONS
    # ===========================================
    print("--- Aggregations ---\n")
    
    # Count, Sum, Avg
    print("Department statistics:")
    spark.sql("""
        SELECT 
            department,
            COUNT(*) as employee_count,
            SUM(salary) as total_salary,
            AVG(salary) as avg_salary,
            MIN(salary) as min_salary,
            MAX(salary) as max_salary
        FROM employees
        GROUP BY department
        ORDER BY employee_count DESC
    """).show()
    
    # HAVING clause
    print("Departments with avg salary > 70000:")
    spark.sql("""
        SELECT 
            department,
            COUNT(*) as count,
            ROUND(AVG(salary), 2) as avg_salary
        FROM employees
        GROUP BY department
        HAVING AVG(salary) > 70000
    """).show()
    
    # ===========================================
    # 4. JOIN OPERATIONS
    # ===========================================
    print("--- JOIN Operations ---\n")
    
    # INNER JOIN
    print("Employees with department info (INNER JOIN):")
    spark.sql("""
        SELECT 
            e.name,
            e.department,
            e.salary,
            d.location,
            d.budget
        FROM employees e
        INNER JOIN departments d 
            ON e.department = d.dept_name
    """).show()
    
    # LEFT JOIN
    print("All departments with employee count (LEFT JOIN):")
    spark.sql("""
        SELECT 
            d.dept_name,
            d.location,
            COUNT(e.emp_id) as employee_count
        FROM departments d
        LEFT JOIN employees e 
            ON d.dept_name = e.department
        GROUP BY d.dept_name, d.location
    """).show()
    
    # Multiple joins
    print("Employees with manager names:")
    spark.sql("""
        SELECT 
            e.name as employee,
            e.department,
            e.salary,
            m.manager_name,
            m.title as manager_title
        FROM employees e
        JOIN managers m 
            ON e.manager_id = m.manager_id
        ORDER BY e.department, e.salary DESC
    """).show()
    
    # ===========================================
    # 5. SUBQUERIES
    # ===========================================
    print("--- Subqueries ---\n")
    
    # Subquery in WHERE
    print("Employees earning above average:")
    spark.sql("""
        SELECT name, department, salary
        FROM employees
        WHERE salary > (SELECT AVG(salary) FROM employees)
        ORDER BY salary DESC
    """).show()
    
    # Correlated subquery
    print("Employees earning above department average:")
    spark.sql("""
        SELECT e.name, e.department, e.salary
        FROM employees e
        WHERE e.salary > (
            SELECT AVG(e2.salary) 
            FROM employees e2 
            WHERE e2.department = e.department
        )
        ORDER BY e.department, e.salary DESC
    """).show()
    
    # Subquery in FROM
    print("Department stats from subquery:")
    spark.sql("""
        SELECT *
        FROM (
            SELECT 
                department,
                COUNT(*) as count,
                AVG(salary) as avg_sal
            FROM employees
            GROUP BY department
        ) dept_stats
        WHERE count > 1
    """).show()
    
    # ===========================================
    # 6. COMMON TABLE EXPRESSIONS (CTEs)
    # ===========================================
    print("--- CTEs (WITH clause) ---\n")
    
    # Simple CTE
    print("Using CTE for department averages:")
    spark.sql("""
        WITH dept_avg AS (
            SELECT 
                department,
                AVG(salary) as avg_salary
            FROM employees
            GROUP BY department
        )
        SELECT 
            e.name,
            e.department,
            e.salary,
            ROUND(da.avg_salary, 2) as dept_avg,
            ROUND(e.salary - da.avg_salary, 2) as vs_avg
        FROM employees e
        JOIN dept_avg da ON e.department = da.department
        ORDER BY vs_avg DESC
    """).show()
    
    # Multiple CTEs
    print("Multiple CTEs - ranking employees:")
    spark.sql("""
        WITH 
        dept_stats AS (
            SELECT 
                department,
                AVG(salary) as avg_sal,
                MAX(salary) as max_sal
            FROM employees
            GROUP BY department
        ),
        ranked_emps AS (
            SELECT 
                *,
                RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
            FROM employees
        )
        SELECT 
            r.name,
            r.department,
            r.salary,
            r.dept_rank,
            ROUND(d.avg_sal, 0) as dept_avg
        FROM ranked_emps r
        JOIN dept_stats d ON r.department = d.department
        WHERE r.dept_rank <= 2
        ORDER BY r.department, r.dept_rank
    """).show()
    
    # ===========================================
    # 7. WINDOW FUNCTIONS IN SQL
    # ===========================================
    print("--- Window Functions ---\n")
    
    # ROW_NUMBER, RANK, DENSE_RANK
    print("Ranking functions:")
    spark.sql("""
        SELECT 
            name,
            department,
            salary,
            ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num,
            RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank,
            DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dense_rank
        FROM employees
    """).show()
    
    # Running totals
    print("Running totals:")
    spark.sql("""
        SELECT 
            name,
            department,
            salary,
            SUM(salary) OVER (PARTITION BY department ORDER BY name) as running_total,
            SUM(salary) OVER (PARTITION BY department) as dept_total
        FROM employees
        ORDER BY department, name
    """).show()
    
    # LAG and LEAD
    print("LAG and LEAD (previous/next salary):")
    spark.sql("""
        SELECT 
            name,
            department,
            salary,
            LAG(salary, 1) OVER (PARTITION BY department ORDER BY salary) as prev_salary,
            LEAD(salary, 1) OVER (PARTITION BY department ORDER BY salary) as next_salary
        FROM employees
        ORDER BY department, salary
    """).show()
    
    # ===========================================
    # 8. USEFUL SQL FUNCTIONS
    # ===========================================
    print("--- Useful SQL Functions ---\n")
    
    # String functions
    print("String functions:")
    spark.sql("""
        SELECT 
            name,
            UPPER(name) as upper_name,
            LENGTH(name) as name_length,
            CONCAT(name, ' - ', department) as full_desc
        FROM employees
        LIMIT 3
    """).show(truncate=False)
    
    # CASE WHEN
    print("CASE WHEN:")
    spark.sql("""
        SELECT 
            name,
            salary,
            CASE 
                WHEN salary >= 80000 THEN 'Senior'
                WHEN salary >= 70000 THEN 'Mid'
                ELSE 'Junior'
            END as level
        FROM employees
    """).show()
    
    # COALESCE and NULLIF
    print("Handling NULLs:")
    null_data = spark.createDataFrame([
        (1, "Alice", 100),
        (2, None, 200),
        (3, "Charlie", None)
    ], ["id", "name", "value"])
    null_data.createOrReplaceTempView("null_test")
    
    spark.sql("""
        SELECT 
            id,
            COALESCE(name, 'Unknown') as name,
            COALESCE(value, 0) as value
        FROM null_test
    """).show()
    
    # ===========================================
    # 9. SAVE SQL RESULTS
    # ===========================================
    print("--- Save SQL Results ---\n")
    
    # SQL result is a DataFrame - can use DataFrame operations
    result = spark.sql("""
        SELECT department, COUNT(*) as count, AVG(salary) as avg_sal
        FROM employees
        GROUP BY department
    """)
    
    # Convert to Pandas
    pandas_df = result.toPandas()
    print("Converted to Pandas:")
    print(pandas_df)
    print()
    
    # Collect as list
    rows = result.collect()
    print("As Python list:")
    for row in rows:
        print(f"  {row.department}: {row.count} employees, avg ${row.avg_sal:.0f}")
    
    # ===========================================
    # CLEANUP
    # ===========================================
    spark.stop()
    print("\n" + "="*60)
    print("  SQL Operations Complete! 🎉")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
