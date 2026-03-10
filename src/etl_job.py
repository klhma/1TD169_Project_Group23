"""
etl_job.py
----------
ETL (Extract, Transform, Load) job: reads raw data, cleans it,
and writes the result to the cleaned data path defined in config.py.

Usage:
    spark-submit src/etl_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import config


def create_spark_session() -> SparkSession:
    """Create and return a SparkSession."""
    return (
        SparkSession.builder
        .appName(f"{config.APP_NAME} - ETL")
        .master(config.SPARK_MASTER)
        .getOrCreate()
    )


def extract(spark: SparkSession, path: str):
    """Read raw data from *path* and return a DataFrame."""
    return spark.read.json(path)


def transform(df):
    """
    Clean and transform the raw DataFrame.

    Steps performed:
    - Drop rows where all values are null
    - Trim whitespace from all string columns
    - Cast numeric columns to appropriate types (extend as needed)
    """
    # Drop completely empty rows
    df = df.dropna(how="all")

    # Trim whitespace from string columns
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    return df


def load(df, path: str, fmt: str = config.OUTPUT_FORMAT) -> None:
    """Write the transformed DataFrame to *path* in the given format."""
    # Allowing Spark to automatically handle partitioning for large data writes.
    writer = df.write.mode("overwrite")
    if fmt == "parquet":
        writer.parquet(path)
    else:
        writer.json(path)


def main():
    spark = create_spark_session()
    try:
        print(f"[ETL] Reading raw data from: {config.RAW_DATA_PATH}")
        raw_df = extract(spark, config.RAW_DATA_PATH)

        print("[ETL] Transforming data ...")
        clean_df = transform(raw_df)

        print(f"[ETL] Writing cleaned data to: {config.CLEANED_DATA_PATH}")
        load(clean_df, config.CLEANED_DATA_PATH)

        print("[ETL] Done.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()