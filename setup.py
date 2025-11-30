"""
Spark Examples Package Setup
"""

from setuptools import setup, find_packages

setup(
    name="spark-examples",
    version="1.0.0",
    description="Apache Spark learning examples for local development",
    author="Your Name",
    python_requires=">=3.9",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.5.0",
        "pandas>=2.0.0",
        "pyarrow>=14.0.0",
    ],
)
