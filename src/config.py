"""
config.py
---------
Central configuration for paths, constants, and cluster settings.
Edit this file to match your environment before running any jobs.
"""

# ---------------------------------------------------------------------------
# Data paths
# ---------------------------------------------------------------------------

# Root directory for raw input data (local path or HDFS/S3 URI)
RAW_DATA_PATH = "data/raw/"

# Output directory for cleaned/transformed data
CLEANED_DATA_PATH = "data/cleaned/"

# Output directory for analysis results
OUTPUT_PATH = "data/output/"

# ---------------------------------------------------------------------------
# Spark / cluster settings
# ---------------------------------------------------------------------------

# Spark master URL. Use "local[*]" for local mode, or e.g. "spark://master:7077"
SPARK_MASTER = "local[*]"

APP_NAME = "1TD169_Project_Group23"

# Number of partitions to use when repartitioning DataFrames
NUM_PARTITIONS = 8

# ---------------------------------------------------------------------------
# Benchmark / scaling test settings
# ---------------------------------------------------------------------------

# Data sizes (in MB) for the three scaling tests
BENCHMARK_SIZES_MB = [100, 500, 1000]

# Number of worker nodes to test with (for strong/weak scaling)
BENCHMARK_WORKER_COUNTS = [1, 2, 4]

# ---------------------------------------------------------------------------
# File format settings
# ---------------------------------------------------------------------------

# "parquet" or "csv"
OUTPUT_FORMAT = "parquet"
