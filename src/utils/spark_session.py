"""
Spark Session Utility

Provides a centralized way to create and manage SparkSession
with sensible defaults for local development.
"""

from pyspark.sql import SparkSession
import os

# Global SparkSession reference
_spark_session = None


def get_spark_session(
    app_name: str = "SparkExample",
    master: str = "local[*]",
    memory: str = "2g",
    log_level: str = "WARN"
) -> SparkSession:
    """
    Get or create a SparkSession with local development settings.
    
    Args:
        app_name: Name of the Spark application
        master: Spark master URL (default: local[*] uses all cores)
        memory: Driver memory allocation (default: 2g)
        log_level: Logging level (default: WARN to reduce noise)
    
    Returns:
        SparkSession instance
    
    Example:
        >>> spark = get_spark_session("MyApp")
        >>> df = spark.read.csv("data.csv")
    """
    global _spark_session
    
    if _spark_session is None:
        # Set environment variable to suppress some warnings
        os.environ['PYSPARK_PYTHON'] = 'python3'
        
        _spark_session = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            # Memory settings
            .config("spark.driver.memory", memory)
            .config("spark.sql.shuffle.partitions", "8")  # Reduce for local
            # Performance settings for local dev
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            # Disable Hive support for simplicity
            .config("spark.sql.catalogImplementation", "in-memory")
            # Suppress verbose logging
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        
        # Set log level
        _spark_session.sparkContext.setLogLevel(log_level)
        
        print(f"✓ SparkSession created: {app_name}")
        print(f"  - Master: {master}")
        print(f"  - Memory: {memory}")
        print(f"  - Spark Version: {_spark_session.version}")
        print()
    
    return _spark_session


def stop_spark():
    """
    Stop the SparkSession and clean up resources.
    
    Example:
        >>> stop_spark()
    """
    global _spark_session
    
    if _spark_session is not None:
        _spark_session.stop()
        _spark_session = None
        print("✓ SparkSession stopped")


def print_section(title: str):
    """Print a formatted section header."""
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)
    print()


def print_subsection(title: str):
    """Print a formatted subsection header."""
    print()
    print(f"--- {title} ---")
    print()
