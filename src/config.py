"""
config.py
---------
Central configuration for paths, constants, and cluster settings.
Edit this file to match your environment before running any jobs.
"""

# ---------------------------------------------------------------------------
# Data paths
# ---------------------------------------------------------------------------

RAW_DATA_PATH = "/data/reddit/corpus-webis-tldr-17.json"

CLEANED_DATA_PATH = "/data/cleaned_reddit/"

OUTPUT_PATH = "/data/output/"

# ---------------------------------------------------------------------------
# Spark / cluster settings
# ---------------------------------------------------------------------------

SPARK_MASTER = "spark://g23-master:7077"

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