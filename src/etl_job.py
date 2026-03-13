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
from pyspark.ml.feature import StopWordsRemover
import config
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Explicit schema definition for input JSON data to ensure consistent parsing and better performance
schema = StructType([
    StructField("author", StringType(), True),
    StructField("body", StringType(), True),
    StructField("normalizedBody", StringType(), True),
    StructField("content", StringType(), True),
    StructField("content_len", IntegerType(), True),
    StructField("summary", StringType(), True),
    StructField("summary_len", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("subreddit_id", StringType(), True),
])

def create_spark_session() -> SparkSession:
    """Create and return a SparkSession."""
    return (
        SparkSession.builder
        .appName(f"{config.APP_NAME} - ETL")
        .master(config.SPARK_MASTER)
        .getOrCreate()
    )


def extract(spark: SparkSession, path: str):
    """Read raw JSON data from the given path using the explicit schema."""
    return spark.read.json(path, schema=schema)

def transform(df):
    """
    Clean and transform the raw DataFrame.

    Steps performed:
    - Drop rows where all values are null
    - Trim whitespace from all string columns
    - Tokenize content, remove stopwords, and clean tokens
    """
    # Drop completely empty rows
    df = df.dropna(how="all")

    # Trim whitespace from string columns
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # Tokenize content column to create content_tokens
    df = df.withColumn("content_tokens", F.split(F.lower(F.col("content")), r"\s+"))
    
    # Remove stopwords from content_tokens
    remover = StopWordsRemover(inputCol="content_tokens", outputCol="filtered_tokens")
    df = remover.transform(df)
    
    # Clean tokens: lowercase and remove non-alphanumeric characters
    df = df.withColumn(
        "content_tokens",
        F.expr("transform(filtered_tokens, x -> lower(regexp_replace(x, '[^a-zA-Z0-9]', '')))")
    )
    df = df.drop("filtered_tokens")
    return df


def load(df, path: str, fmt: str = config.OUTPUT_FORMAT) -> None:
    """Write the transformed DataFrame to the given path in the specified format."""
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