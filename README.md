# Apache Spark Examples - Local Development Environment

A fully working Apache Spark example project for learning and experimentation on macOS.

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Project Structure](#-project-structure)
- [Running Examples](#-running-examples)
- [Examples Included](#-examples-included)
- [Troubleshooting](#-troubleshooting)

---

## 🚀 Quick Start

```bash
# 1. Setup environment (IMPORTANT - run this first!)
source setup_env.sh

# 2. Install dependencies
make install

# 3. Run your first Spark job
make run-hello

# 4. Run all examples
make run-all
```

### One-liner Quick Start

```bash
source setup_env.sh && make install && make run-hello
```

---

## 📦 Prerequisites

- **macOS** (Intel or Apple Silicon)
- **Python 3.9+**
- **Java 11 or 17** (required by Spark) - **⚠️ Java 21+ is NOT compatible!**

### ⚠️ IMPORTANT: Java Version Compatibility

**Spark 3.5.x requires Java 8, 11, or 17.** If you have Java 21 or higher, install Java 17:

```bash
# Install Java 17 via Homebrew
brew install openjdk@17

# Set JAVA_HOME (add to ~/.zshrc or ~/.bash_profile for persistence)
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

# Apply changes
source ~/.zshrc
```

### Quick Fix (Temporary for This Session)

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17 && export PATH="$JAVA_HOME/bin:$PATH"
```

### Check Java Installation

```bash
java -version  # Should show version 11 or 17
```

---

## 🔧 Installation

### Option 1: Using Make (Recommended)

```bash
make install
```

### Option 2: Manual Installation

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Download sample data
python scripts/download_data.py
```

---

## 📁 Project Structure

```
spark-examples/
├── README.md                 # This file
├── Makefile                  # Easy commands
├── requirements.txt          # Python dependencies
├── setup.py                  # Package setup
│
├── data/                     # Sample datasets
│   ├── customers.csv
│   ├── transactions.csv
│   └── products.json
│
├── src/                      # Source code
│   ├── __init__.py
│   ├── utils/               # Utility functions
│   │   ├── __init__.py
│   │   └── spark_session.py
│   │
│   └── examples/            # Example scripts
│       ├── __init__.py
│       ├── 01_hello_spark.py
│       ├── 02_dataframe_basics.py
│       ├── 03_sql_queries.py
│       ├── 04_aggregations.py
│       ├── 05_joins.py
│       ├── 06_window_functions.py
│       ├── 07_etl_pipeline.py
│       └── 08_performance_tips.py
│
├── notebooks/               # Jupyter notebooks
│   └── spark_playground.ipynb
│
├── output/                  # Output directory
│
└── scripts/                 # Helper scripts
    ├── download_data.py
    └── run_example.py
```

---

## ▶️ Running Examples

### Using Make

```bash
# Run specific examples
make run-hello          # 01_hello_spark.py
make run-basics         # 02_dataframe_basics.py
make run-sql            # 03_sql_queries.py
make run-agg            # 04_aggregations.py
make run-joins          # 05_joins.py
make run-window         # 06_window_functions.py
make run-etl            # 07_etl_pipeline.py
make run-perf           # 08_performance_tips.py

# Run all examples
make run-all

# Start Jupyter notebook
make notebook

# Interactive PySpark shell
make shell
```

### Direct Python Execution

```bash
source venv/bin/activate
python src/examples/01_hello_spark.py
```

---

## 📚 Examples Included

### 1. Hello Spark (`01_hello_spark.py`)
- Creating a SparkSession
- Creating DataFrames from Python lists
- Basic show() and count()

### 2. DataFrame Basics (`02_dataframe_basics.py`)
- Reading CSV, JSON, Parquet
- Selecting columns
- Filtering rows
- Adding/modifying columns

### 3. SQL Queries (`03_sql_queries.py`)
- Registering DataFrames as tables
- Running SQL queries
- Mixing DataFrame API with SQL

### 4. Aggregations (`04_aggregations.py`)
- GroupBy operations
- Multiple aggregations
- Having clauses

### 5. Joins (`05_joins.py`)
- Inner, left, right, full outer joins
- Broadcast joins for performance
- Handling column name conflicts

### 6. Window Functions (`06_window_functions.py`)
- Running totals
- Ranking
- Lag/Lead functions
- Moving averages

### 7. ETL Pipeline (`07_etl_pipeline.py`)
- Full Extract-Transform-Load example
- Data quality checks
- Writing partitioned output

### 8. Performance Tips (`08_performance_tips.py`)
- Caching
- Broadcast variables
- Partitioning strategies
- Execution plan analysis

---

## 🐛 Troubleshooting

### Java Not Found

```bash
# Check Java installation
java -version

# If using Homebrew
brew install openjdk@17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

### PySpark ImportError

```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall pyspark
pip install --upgrade pyspark
```

### Memory Issues

```bash
# Set Spark memory in your script or environment
export SPARK_DRIVER_MEMORY=4g
```

Or in code:
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

### Permission Issues on macOS

```bash
# If you get security warnings
xattr -d com.apple.quarantine venv/lib/python*/site-packages/pyspark/bin/*
```

---

## 📖 Learning Path

1. **Start here**: `01_hello_spark.py` - Get Spark running
2. **Learn basics**: `02_dataframe_basics.py` - Core operations
3. **Add SQL**: `03_sql_queries.py` - SQL integration
4. **Aggregate**: `04_aggregations.py` - Group and summarize
5. **Combine data**: `05_joins.py` - Join datasets
6. **Advanced**: `06_window_functions.py` - Analytics functions
7. **Real-world**: `07_etl_pipeline.py` - Full pipeline
8. **Optimize**: `08_performance_tips.py` - Make it fast

---

## 🔗 Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

---

## License

MIT License - Feel free to use for learning and projects!
