# Makefile for Apache Spark Examples
# Run `make help` for available commands

.PHONY: help init install clean run-all run-hello run-basics run-sql run-agg run-joins run-window run-etl run-perf run-files notebook shell

# Use asdf Python
PYTHON_VERSION := 3.12.5
JAVA_VERSION := openjdk-17
PYTHON := $(HOME)/.asdf/installs/python/$(PYTHON_VERSION)/bin/python3
VENV := venv
PIP := $(VENV)/bin/pip
PYTHON_VENV := $(VENV)/bin/python
JUPYTER := $(VENV)/bin/jupyter
PYSPARK := $(VENV)/bin/pyspark

# Spark requires Java 17 (not 21+)
export JAVA_HOME := $(HOME)/.asdf/installs/java/$(JAVA_VERSION)
export PATH := $(JAVA_HOME)/bin:$(PATH)
export PYSPARK_PYTHON := $(PYTHON_VENV)
export PYSPARK_DRIVER_PYTHON := $(PYTHON_VENV)

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(GREEN)Apache Spark Examples - Available Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-15s$(NC) %s\n", $$1, $$2}'
	@echo ""

init: ## Check and install all prerequisites (asdf, Python, Java)
	@echo "$(GREEN)Checking prerequisites...$(NC)"
	@echo ""
	@# Check asdf
	@echo "$(YELLOW)Checking asdf...$(NC)"
	@command -v asdf >/dev/null 2>&1 || { \
		echo "$(RED)asdf not found! Please install asdf first:$(NC)"; \
		echo "  brew install asdf"; \
		echo "  Then add to your shell: echo '. $$(brew --prefix asdf)/libexec/asdf.sh' >> ~/.zshrc"; \
		exit 1; \
	}
	@echo "  $(GREEN)✓ asdf is installed$(NC)"
	@echo ""
	@# Check/install Python plugin
	@echo "$(YELLOW)Checking asdf Python plugin...$(NC)"
	@asdf plugin list 2>/dev/null | grep -q python || { \
		echo "  Installing asdf Python plugin..."; \
		asdf plugin add python; \
	}
	@echo "  $(GREEN)✓ Python plugin installed$(NC)"
	@echo ""
	@# Check/install Java plugin
	@echo "$(YELLOW)Checking asdf Java plugin...$(NC)"
	@asdf plugin list 2>/dev/null | grep -q java || { \
		echo "  Installing asdf Java plugin..."; \
		asdf plugin add java; \
	}
	@echo "  $(GREEN)✓ Java plugin installed$(NC)"
	@echo ""
	@# Check/install Python version
	@echo "$(YELLOW)Checking Python $(PYTHON_VERSION)...$(NC)"
	@asdf list python 2>/dev/null | grep -q $(PYTHON_VERSION) || { \
		echo "  Installing Python $(PYTHON_VERSION) (this may take a few minutes)..."; \
		asdf install python $(PYTHON_VERSION); \
	}
	@echo "  $(GREEN)✓ Python $(PYTHON_VERSION) installed$(NC)"
	@echo ""
	@# Check/install Java version
	@echo "$(YELLOW)Checking Java $(JAVA_VERSION)...$(NC)"
	@asdf list java 2>/dev/null | grep -q $(JAVA_VERSION) || { \
		echo "  Installing Java $(JAVA_VERSION) (this may take a few minutes)..."; \
		asdf install java $(JAVA_VERSION); \
	}
	@echo "  $(GREEN)✓ Java $(JAVA_VERSION) installed$(NC)"
	@echo ""
	@# Set local versions for this project
	@echo "$(YELLOW)Setting local versions for this project...$(NC)"
	@asdf set python $(PYTHON_VERSION)
	@asdf set java $(JAVA_VERSION)
	@echo "  $(GREEN)✓ Local versions set$(NC)"
	@echo ""
	@echo "$(GREEN)All prerequisites installed!$(NC)"
	@echo ""
	@echo "Python: $$($(PYTHON) --version 2>&1)"
	@echo "Java:   $$($(JAVA_HOME)/bin/java -version 2>&1 | head -1)"
	@echo ""
	@echo "$(GREEN)Run 'make install' to set up the virtual environment.$(NC)"

install: ## Install all dependencies and set up environment
	@echo "$(GREEN)Creating virtual environment...$(NC)"
	@rm -rf $(VENV)
	$(PYTHON) -m venv $(VENV)
	@test -f $(PIP) || { echo "$(RED)Failed to create virtual environment!$(NC)"; exit 1; }
	@echo "$(GREEN)Installing dependencies...$(NC)"
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements.txt
	@echo "$(GREEN)Setup complete!$(NC)"
	@echo "$(GREEN)Installation complete!$(NC)"
	@echo ""
	@echo "To activate the virtual environment:"
	@echo "  source venv/bin/activate"

clean: ## Remove virtual environment and output files
	@echo "$(YELLOW)Cleaning up...$(NC)"
	rm -rf $(VENV)
	rm -rf output/*
	rm -rf __pycache__
	rm -rf src/__pycache__
	rm -rf src/*/__pycache__
	rm -rf .ipynb_checkpoints
	rm -rf spark-warehouse
	rm -rf metastore_db
	rm -rf derby.log
	@echo "$(GREEN)Cleaned!$(NC)"

# Run individual examples
run-hello: ## Run Example 1: Hello Spark
	@echo "$(GREEN)Running: Hello Spark$(NC)"
	$(PYTHON_VENV) src/examples/01_hello_spark.py

run-basics: ## Run Example 2: DataFrame Basics
	@echo "$(GREEN)Running: DataFrame Basics$(NC)"
	$(PYTHON_VENV) src/examples/02_dataframe_basics.py

run-sql: ## Run Example 3: SQL Operations
	@echo "$(GREEN)Running: SQL Operations$(NC)"
	$(PYTHON_VENV) src/examples/03_sql_operations.py

run-agg: ## Run Example 4: Aggregations
	@echo "$(GREEN)Running: Aggregations$(NC)"
	$(PYTHON_VENV) src/examples/04_aggregations.py

run-joins: ## Run Example 5: Joins
	@echo "$(GREEN)Running: Joins$(NC)"
	$(PYTHON_VENV) src/examples/05_joins.py

run-window: ## Run Example 6: Window Functions
	@echo "$(GREEN)Running: Window Functions$(NC)"
	$(PYTHON_VENV) src/examples/06_window_functions.py

run-etl: ## Run Example 7: ETL Pipeline
	@echo "$(GREEN)Running: ETL Pipeline$(NC)"
	$(PYTHON_VENV) src/examples/07_etl_pipeline.py

run-perf: ## Run Example 8: Performance Tips
	@echo "$(GREEN)Running: Performance Tips$(NC)"
	$(PYTHON_VENV) src/examples/08_performance.py

run-all: ## Run all examples in sequence
	@echo "$(GREEN)Running all examples...$(NC)"
	@echo ""
	$(MAKE) run-hello
	@echo ""
	$(MAKE) run-basics
	@echo ""
	$(MAKE) run-sql
	@echo ""
	$(MAKE) run-agg
	@echo ""
	$(MAKE) run-joins
	@echo ""
	$(MAKE) run-window
	@echo ""
	$(MAKE) run-etl
	@echo ""
	$(MAKE) run-perf
	@echo ""
	@echo "$(GREEN)All examples completed!$(NC)"

notebook: ## Start Jupyter notebook server
	@echo "$(GREEN)Starting Jupyter Notebook...$(NC)"
	$(JUPYTER) notebook notebooks/

shell: ## Start interactive PySpark shell
	@echo "$(GREEN)Starting PySpark shell...$(NC)"
	@echo "Type 'exit()' or Ctrl+D to quit"
	$(PYSPARK)

test: ## Run tests (if any)
	@echo "$(GREEN)Running tests...$(NC)"
	$(PYTHON_VENV) -m pytest tests/ -v

check-java: ## Check Java installation
	@echo "$(GREEN)Checking Java installation...$(NC)"
	@java -version 2>&1 || echo "$(YELLOW)Java not found! Please install Java 11 or 17$(NC)"
	@echo ""
	@echo "JAVA_HOME: $${JAVA_HOME:-Not set}"
