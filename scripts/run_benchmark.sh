#!/usr/bin/env bash
# run_benchmark.sh
# Runs 3 scaling tests (small, medium, large dataset) and records timing.
# Edit the variables below to match your environment.
#
# NOTE: This script uses `date +%s%N` for nanosecond-precision timing,
# which requires GNU coreutils (available on Linux). On macOS, install
# GNU coreutils via Homebrew: `brew install coreutils` and use `gdate`.

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SRC_DIR="$(cd "$(dirname "$0")/../src" && pwd)"
RESULTS_DIR="$(cd "$(dirname "$0")/.." && pwd)/benchmark_results"
SPARK_SUBMIT="spark-submit"

# Data sizes in MB for the three tests
SIZES=(100 500 1000)

# Optional: path to a script that generates synthetic data of a given size
# Set to "" to skip data generation (assumes data already exists)
DATA_GEN_SCRIPT=""  # e.g., "scripts/generate_data.py"

# ---------------------------------------------------------------------------
# Prepare results directory
# ---------------------------------------------------------------------------
mkdir -p "${RESULTS_DIR}"
REPORT_FILE="${RESULTS_DIR}/benchmark_$(date +%Y%m%d_%H%M%S).txt"

echo "Benchmark started at $(date)" | tee "${REPORT_FILE}"
echo "Results will be saved to: ${REPORT_FILE}"
echo "------------------------------------------------------------" | tee -a "${REPORT_FILE}"

# ---------------------------------------------------------------------------
# Run three scaling tests
# ---------------------------------------------------------------------------
for SIZE_MB in "${SIZES[@]}"; do
    echo "" | tee -a "${REPORT_FILE}"
    echo "=== Test: dataset size = ${SIZE_MB} MB ===" | tee -a "${REPORT_FILE}"

    # Optionally generate data
    if [[ -n "${DATA_GEN_SCRIPT}" ]]; then
        echo "  Generating ${SIZE_MB} MB of synthetic data ..." | tee -a "${REPORT_FILE}"
        python "${DATA_GEN_SCRIPT}" --size "${SIZE_MB}"
    fi

    # Run ETL job and measure time
    echo "  Running ETL job ..." | tee -a "${REPORT_FILE}"
    ETL_START=$(date +%s%N)
    "${SPARK_SUBMIT}" "${SRC_DIR}/etl_job.py"
    ETL_END=$(date +%s%N)
    ETL_ELAPSED=$(( (ETL_END - ETL_START) / 1000000 ))
    echo "  ETL elapsed: ${ETL_ELAPSED} ms" | tee -a "${REPORT_FILE}"

    # Run Analysis job and measure time
    echo "  Running Analysis job ..." | tee -a "${REPORT_FILE}"
    ANALYSIS_START=$(date +%s%N)
    "${SPARK_SUBMIT}" "${SRC_DIR}/analysis_job.py"
    ANALYSIS_END=$(date +%s%N)
    ANALYSIS_ELAPSED=$(( (ANALYSIS_END - ANALYSIS_START) / 1000000 ))
    echo "  Analysis elapsed: ${ANALYSIS_ELAPSED} ms" | tee -a "${REPORT_FILE}"

    TOTAL_ELAPSED=$(( ETL_ELAPSED + ANALYSIS_ELAPSED ))
    echo "  Total elapsed for ${SIZE_MB} MB: ${TOTAL_ELAPSED} ms" | tee -a "${REPORT_FILE}"
done

echo "" | tee -a "${REPORT_FILE}"
echo "------------------------------------------------------------" | tee -a "${REPORT_FILE}"
echo "Benchmark finished at $(date)" | tee -a "${REPORT_FILE}"
echo "Full report: ${REPORT_FILE}"
