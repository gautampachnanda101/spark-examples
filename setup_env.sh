#!/bin/bash
# setup_env.sh - Setup environment for running Spark examples
# Source this file: source setup_env.sh

# Set Java 17 (required for Spark)
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

# Set Python paths for PySpark (avoids asdf issues)
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Verify setup
echo "Environment configured:"
echo "  JAVA_HOME: $JAVA_HOME"
echo "  Java version: $(java -version 2>&1 | head -1)"
echo "  Python: $PYSPARK_PYTHON"
echo "  Python version: $(/usr/bin/python3 --version)"
echo ""
echo "Ready to run Spark examples!"
echo "Try: make run-hello"
