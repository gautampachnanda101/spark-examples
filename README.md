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
# Setup everything (Python, Java, dependencies) - one command!
make init

# Run your first Spark job
make run-hello

# Run all examples
make run-all

# See all available commands
make help
```

### One-liner Quick Start

```bash
make init && make run-hello
```

---

## 📦 Prerequisites

- **macOS** (Intel or Apple Silicon)
- **[asdf](https://asdf-vm.com/)** - Version manager for Python and Java

### Installing asdf (if not already installed)

```bash
# Install asdf via Homebrew
brew install asdf

# Add to your shell (zsh)
echo '. $(brew --prefix asdf)/libexec/asdf.sh' >> ~/.zshrc
source ~/.zshrc
```

### Automatic Setup with `make init`

The `make init` command will automatically:
- ✅ Install asdf Python and Java plugins
- ✅ Install Python 3.12.5
- ✅ Install Java OpenJDK 17 (required by Spark)
- ✅ Set local versions for this project

```bash
make init
```

### ⚠️ Java Version Compatibility

**Spark 3.5.x requires Java 8, 11, or 17.** Java 21+ is NOT compatible!

The Makefile automatically uses Java 17 via asdf. To check your Java installation:

```bash
make check-java
```

---

## 🔧 Installation

### Using Make (Recommended)

```bash
# First time setup
make init      # Install prerequisites via asdf
make install   # Create venv and install packages
```

### Manual Installation

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## 📁 Project Structure

```
spark-examples/
├── README.md                 # This file
├── Makefile                  # Easy commands (run `make help`)
├── requirements.txt          # Python dependencies
├── setup.py                  # Package setup
├── pytest.ini                # Pytest configuration
│
├── data/                     # Sample datasets
│   ├── customers.csv
│   ├── employees.csv
│   └── orders.csv
│
├── src/                      # Source code
│   ├── __init__.py
│   ├── utils/                # Utility functions
│   │   ├── __init__.py
│   │   └── spark_session.py
│   │
│   └── examples/             # Example scripts
│       ├── 01_hello_spark.py
│       ├── 02_dataframe_basics.py
│       ├── 03_sql_operations.py
│       ├── 04_aggregations.py
│       ├── 05_joins.py
│       ├── 06_window_functions.py
│       ├── 07_etl_pipeline.py
│       ├── 08_performance.py
│       └── 09_reading_files.py
│
├── notebooks/                # Jupyter notebooks
│   └── 01_spark_tutorial.ipynb
│
├── output/                   # Output directory
│
└── tests/                    # Unit tests
    └── test_spark_basics.py
```

---

## ▶️ Running Examples

### Using Make

```bash
# See all available commands with colored output
make help

# Run specific examples
make run-hello          # 01_hello_spark.py
make run-basics         # 02_dataframe_basics.py
make run-sql            # 03_sql_operations.py
make run-agg            # 04_aggregations.py
make run-joins          # 05_joins.py
make run-window         # 06_window_functions.py
make run-etl            # 07_etl_pipeline.py
make run-perf           # 08_performance.py
make run-files          # 09_reading_files.py

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

### 3. SQL Operations (`03_sql_operations.py`)
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

### 8. Performance (`08_performance.py`)
- Caching
- Broadcast variables
- Partitioning strategies
- Execution plan analysis

### 9. Reading Files (`09_reading_files.py`)
- Reading various file formats
- Schema inference vs explicit schema
- Handling malformed data

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
