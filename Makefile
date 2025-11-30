# Makefile for Apache Spark Examples
# Run `make help` for available commands

.PHONY: help init install clean run-all run-hello run-basics run-sql run-agg run-joins run-window run-etl run-perf run-files notebook shell test lint format check-java

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
BLUE := \033[0;34m
CYAN := \033[0;36m
BOLD := \033[1m
NC := \033[0m # No Color

help: ## Show this help message
	@echo ""
	@echo "$(BOLD)$(GREEN)╔══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(BOLD)$(GREEN)║         🚀 Apache Spark Examples - Make Commands 🚀          ║$(NC)"
	@echo "$(BOLD)$(GREEN)╚══════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(BOLD)$(BLUE)📦 SETUP$(NC)"
	@echo "  $(YELLOW)init$(NC)            Setup everything (Python, Java, dependencies)"
	@echo "  $(YELLOW)install$(NC)         Reinstall virtual environment"
	@echo "  $(YELLOW)clean$(NC)           Remove venv and generated files"
	@echo ""
	@echo "$(BOLD)$(BLUE)▶️  RUN EXAMPLES$(NC)"
	@echo "  $(YELLOW)run-hello$(NC)       Example 1: Hello Spark - Getting started"
	@echo "  $(YELLOW)run-basics$(NC)      Example 2: DataFrame Basics - Core operations"
	@echo "  $(YELLOW)run-sql$(NC)         Example 3: SQL Operations - SQL integration"
	@echo "  $(YELLOW)run-agg$(NC)         Example 4: Aggregations - Group and summarize"
	@echo "  $(YELLOW)run-joins$(NC)       Example 5: Joins - Combine datasets"
	@echo "  $(YELLOW)run-window$(NC)      Example 6: Window Functions - Analytics"
	@echo "  $(YELLOW)run-etl$(NC)         Example 7: ETL Pipeline - Full pipeline"
	@echo "  $(YELLOW)run-perf$(NC)        Example 8: Performance - Optimization tips"
	@echo "  $(YELLOW)run-files$(NC)       Example 9: Reading Files - File formats"
	@echo "  $(YELLOW)run-all$(NC)         Run all examples in sequence"
	@echo ""
	@echo "$(BOLD)$(BLUE)🛠️  TOOLS$(NC)"
	@echo "  $(YELLOW)notebook$(NC)        Start Jupyter notebook server"
	@echo "  $(YELLOW)shell$(NC)           Start interactive PySpark shell"
	@echo "  $(YELLOW)test$(NC)            Run pytest tests"
	@echo "  $(YELLOW)lint$(NC)            Check code style (black, isort, flake8)"
	@echo "  $(YELLOW)format$(NC)          Auto-format code (black, isort)"
	@echo "  $(YELLOW)check-java$(NC)      Verify Java installation"
	@echo ""
	@echo "$(BOLD)$(CYAN)💡 Quick Start:$(NC)"
	@echo "   make init && make run-hello"
	@echo ""

init: ## Setup everything: install prerequisites (asdf, Python, Java) and dependencies
	@echo ""
	@echo "$(BOLD)$(GREEN)╔══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(BOLD)$(GREEN)║              🚀 Setting up Spark Examples 🚀                 ║$(NC)"
	@echo "$(BOLD)$(GREEN)╚══════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@# Check asdf
	@echo "$(BOLD)$(BLUE)Step 1/6: Checking asdf...$(NC)"
	@command -v asdf >/dev/null 2>&1 || { \
		echo "$(RED)asdf not found! Please install asdf first:$(NC)"; \
		echo "  brew install asdf"; \
		echo "  Then add to your shell: echo '. $$(brew --prefix asdf)/libexec/asdf.sh' >> ~/.zshrc"; \
		exit 1; \
	}
	@echo "  $(GREEN)✓ asdf is installed$(NC)"
	@echo ""
	@# Check/install Python plugin
	@echo "$(BOLD)$(BLUE)Step 2/6: Checking asdf Python plugin...$(NC)"
	@asdf plugin list 2>/dev/null | grep -q python || { \
		echo "  Installing asdf Python plugin..."; \
		asdf plugin add python; \
	}
	@echo "  $(GREEN)✓ Python plugin installed$(NC)"
	@echo ""
	@# Check/install Java plugin
	@echo "$(BOLD)$(BLUE)Step 3/6: Checking asdf Java plugin...$(NC)"
	@asdf plugin list 2>/dev/null | grep -q java || { \
		echo "  Installing asdf Java plugin..."; \
		asdf plugin add java; \
	}
	@echo "  $(GREEN)✓ Java plugin installed$(NC)"
	@echo ""
	@# Check/install Python version
	@echo "$(BOLD)$(BLUE)Step 4/6: Checking Python $(PYTHON_VERSION)...$(NC)"
	@asdf list python 2>/dev/null | grep -q $(PYTHON_VERSION) || { \
		echo "  Installing Python $(PYTHON_VERSION) (this may take a few minutes)..."; \
		asdf install python $(PYTHON_VERSION); \
	}
	@asdf set python $(PYTHON_VERSION)
	@echo "  $(GREEN)✓ Python $(PYTHON_VERSION) ready$(NC)"
	@echo ""
	@# Check/install Java version
	@echo "$(BOLD)$(BLUE)Step 5/6: Checking Java $(JAVA_VERSION)...$(NC)"
	@asdf list java 2>/dev/null | grep -q $(JAVA_VERSION) || { \
		echo "  Installing Java $(JAVA_VERSION) (this may take a few minutes)..."; \
		asdf install java $(JAVA_VERSION); \
	}
	@asdf set java $(JAVA_VERSION)
	@echo "  $(GREEN)✓ Java $(JAVA_VERSION) ready$(NC)"
	@echo ""
	@# Create venv and install dependencies
	@echo "$(BOLD)$(BLUE)Step 6/6: Setting up Python environment...$(NC)"
	@rm -rf $(VENV)
	@$(PYTHON) -m venv $(VENV)
	@test -f $(PIP) || { echo "$(RED)Failed to create virtual environment!$(NC)"; exit 1; }
	@$(VENV)/bin/pip install --upgrade pip -q
	@echo "  Installing dependencies..."
	@$(VENV)/bin/pip install -r requirements.txt -q
	@echo "  $(GREEN)✓ Virtual environment ready$(NC)"
	@echo ""
	@echo "$(BOLD)$(GREEN)╔══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(BOLD)$(GREEN)║                    ✅ Setup Complete!                        ║$(NC)"
	@echo "$(BOLD)$(GREEN)╚══════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "  Python: $$($(PYTHON_VENV) --version 2>&1)"
	@echo "  Java:   $$($(JAVA_HOME)/bin/java -version 2>&1 | head -1)"
	@echo "  PySpark: $$($(PYTHON_VENV) -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null || echo 'installed')"
	@echo ""
	@echo "$(BOLD)$(CYAN)Next steps:$(NC)"
	@echo "  make run-hello    # Run your first Spark job"
	@echo "  make run-all      # Run all examples"
	@echo "  make help         # See all commands"
	@echo ""

install: ## Reinstall virtual environment and dependencies
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

run-files: ## Run Example 9: Reading Files
	@echo "$(GREEN)Running: Reading Files$(NC)"
	$(PYTHON_VENV) src/examples/09_reading_files.py

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
	$(MAKE) run-files
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

lint: ## Check code style (black, isort, flake8)
	@echo "$(GREEN)Checking code style...$(NC)"
	@echo ""
	@# Check if linting tools are installed
	@$(PYTHON_VENV) -c "import black" 2>/dev/null || { \
		echo "$(RED)Linting tools not installed. Run 'make install' first.$(NC)"; \
		exit 1; \
	}
	@echo "$(YELLOW)Checking Black formatting...$(NC)"
	@$(PYTHON_VENV) -m black --check --diff src/ tests/ || { echo "$(RED)Black found formatting issues. Run 'make format' to fix.$(NC)"; exit 1; }
	@echo "  $(GREEN)✓ Black passed$(NC)"
	@echo ""
	@echo "$(YELLOW)Checking isort import sorting...$(NC)"
	@$(PYTHON_VENV) -m isort --check-only --diff src/ tests/ || { echo "$(RED)isort found import issues. Run 'make format' to fix.$(NC)"; exit 1; }
	@echo "  $(GREEN)✓ isort passed$(NC)"
	@echo ""
	@echo "$(YELLOW)Running flake8...$(NC)"
	@$(PYTHON_VENV) -m flake8 src/ tests/ --count --select=E9,F63,F7,F82 --show-source --statistics
	@$(PYTHON_VENV) -m flake8 src/ tests/ --count --exit-zero --max-complexity=10 --max-line-length=120 --statistics
	@echo "  $(GREEN)✓ flake8 passed$(NC)"
	@echo ""
	@echo "$(GREEN)All lint checks passed!$(NC)"

format: ## Auto-format code (black, isort)
	@echo "$(GREEN)Formatting code...$(NC)"
	@echo ""
	@# Check if formatting tools are installed
	@$(PYTHON_VENV) -c "import black" 2>/dev/null || { \
		echo "$(RED)Formatting tools not installed. Run 'make install' first.$(NC)"; \
		exit 1; \
	}
	@echo "$(YELLOW)Running isort...$(NC)"
	$(PYTHON_VENV) -m isort src/ tests/
	@echo "  $(GREEN)✓ Imports sorted$(NC)"
	@echo ""
	@echo "$(YELLOW)Running black...$(NC)"
	$(PYTHON_VENV) -m black src/ tests/
	@echo "  $(GREEN)✓ Code formatted$(NC)"
	@echo ""
	@echo "$(GREEN)Formatting complete!$(NC)"

check-java: ## Check Java installation
	@echo "$(GREEN)Checking Java installation...$(NC)"
	@java -version 2>&1 || echo "$(YELLOW)Java not found! Please install Java 11 or 17$(NC)"
	@echo ""
	@echo "JAVA_HOME: $${JAVA_HOME:-Not set}"
