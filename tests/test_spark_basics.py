"""
Basic tests for Spark Examples
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    session = (
        SparkSession.builder
        .appName("SparkExamplesTest")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestBasicOperations:
    """Test basic Spark operations."""
    
    def test_create_dataframe(self, spark):
        """Test DataFrame creation."""
        data = [(1, "Alice"), (2, "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])
        
        assert df.count() == 2
        assert df.columns == ["id", "name"]
    
    def test_filter(self, spark):
        """Test DataFrame filter."""
        data = [(1, 100), (2, 200), (3, 300)]
        df = spark.createDataFrame(data, ["id", "value"])
        
        filtered = df.filter(df.value > 150)
        assert filtered.count() == 2
    
    def test_aggregation(self, spark):
        """Test DataFrame aggregation."""
        from pyspark.sql.functions import sum as spark_sum, avg
        
        data = [
            ("A", 100),
            ("A", 200),
            ("B", 150)
        ]
        df = spark.createDataFrame(data, ["group", "value"])
        
        result = df.groupBy("group").agg(
            spark_sum("value").alias("total"),
            avg("value").alias("average")
        ).collect()
        
        # Check group A
        group_a = [r for r in result if r.group == "A"][0]
        assert group_a.total == 300
        assert group_a.average == 150.0
    
    def test_join(self, spark):
        """Test DataFrame join."""
        df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        df2 = spark.createDataFrame([(1, 100), (2, 200)], ["id", "salary"])
        
        joined = df1.join(df2, "id")
        
        assert joined.count() == 2
        assert set(joined.columns) == {"id", "name", "salary"}
    
    def test_window_function(self, spark):
        """Test window functions."""
        from pyspark.sql.functions import row_number
        from pyspark.sql.window import Window
        
        data = [
            ("A", 100),
            ("A", 200),
            ("B", 150),
            ("B", 250)
        ]
        df = spark.createDataFrame(data, ["group", "value"])
        
        window = Window.partitionBy("group").orderBy("value")
        result = df.withColumn("rank", row_number().over(window))
        
        assert result.count() == 4
        assert "rank" in result.columns


class TestSQLOperations:
    """Test Spark SQL operations."""
    
    def test_sql_query(self, spark):
        """Test SQL query execution."""
        data = [(1, "Alice", 100), (2, "Bob", 200)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.createOrReplaceTempView("test_table")
        
        result = spark.sql("SELECT name, value FROM test_table WHERE value > 150")
        
        assert result.count() == 1
        assert result.collect()[0].name == "Bob"
    
    def test_cte_query(self, spark):
        """Test CTE (WITH clause) query."""
        data = [(1, 100), (2, 200), (3, 300)]
        df = spark.createDataFrame(data, ["id", "value"])
        df.createOrReplaceTempView("values_table")
        
        result = spark.sql("""
            WITH avg_table AS (
                SELECT AVG(value) as avg_val FROM values_table
            )
            SELECT id, value 
            FROM values_table, avg_table
            WHERE value > avg_val
        """)
        
        assert result.count() == 1
        assert result.collect()[0].id == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
