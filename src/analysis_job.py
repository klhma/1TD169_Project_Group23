"""
analysis_job.py
---------------
Main Spark / MapReduce analysis job.
Reads cleaned data produced by etl_job.py, runs the analytical computations,
and writes results to the output path defined in config.py.

Usage:
    spark-submit src/analysis_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import config


def create_spark_session() -> SparkSession:
    """Create and return a SparkSession."""
    return (
        SparkSession.builder
        .appName(f"{config.APP_NAME} - Analysis")
        .master(config.SPARK_MASTER)
        .getOrCreate()
    )


def load_data(spark: SparkSession, path: str, fmt: str = config.OUTPUT_FORMAT):
    """Load cleaned data from *path* into a DataFrame."""
    if fmt == "parquet":
        return spark.read.parquet(path)
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def run_analysis(df):
    """
    Perform the core analytical computations.

    Replace / extend the placeholder logic below with your actual analysis:
    - Word count (MapReduce-style)
    - Aggregations, joins, window functions, etc.
    """
    # ---------------------------------------------------------------------------
    # Example: word count across all string columns
    # ---------------------------------------------------------------------------
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]

    if string_cols:
        # Concatenate all string columns into one, split into words, count
        word_col = F.concat_ws(" ", *[F.col(c) for c in string_cols])
        words_df = (
            df.select(F.explode(F.split(word_col, r"\s+")).alias("word"))
            .filter(F.col("word") != "")
            .groupBy("word")
            .agg(F.count("*").alias("count"))
            .orderBy(F.col("count").desc())
        )
        return words_df

    # Fallback: return basic row count as a single-row DataFrame
    return df.groupBy().agg(F.count("*").alias("total_rows"))


def save_results(df, path: str, fmt: str = config.OUTPUT_FORMAT) -> None:
    """Write analysis results to *path*."""
    writer = df.coalesce(config.NUM_PARTITIONS).write.mode("overwrite")
    if fmt == "parquet":
        writer.parquet(path)
    else:
        writer.option("header", "true").csv(path)


def main():
    spark = create_spark_session()
    try:
        print(f"[Analysis] Reading cleaned data from: {config.CLEANED_DATA_PATH}")
        df = load_data(spark, config.CLEANED_DATA_PATH)

        print("[Analysis] Running analysis ...")
        result_df = run_analysis(df)
        result_df.show(20, truncate=False)

        print(f"[Analysis] Writing results to: {config.OUTPUT_PATH}")
        save_results(result_df, config.OUTPUT_PATH)

        print("[Analysis] Done.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
