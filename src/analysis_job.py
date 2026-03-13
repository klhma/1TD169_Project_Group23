import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def load_swear_words():
    """Load swear words from the data directory."""
    swear_words_path = os.path.join(os.path.dirname(__file__), "..", "data", "swear_words.txt")
    with open(swear_words_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def reddit_analysis(df, swear_words):
    """
    Find 10 subreddits with most and least amount of swear-word per post,
    considering only subreddits with more than 1000 posts.
    Results are saved to HDFS as CSV files.

    This function:
    - Adds a column 'swear_word_count' to each post, counting the number of swear words present.
    - Aggregates by subreddit to compute:
        * Average swear words per post
        * Total post count
    - Filters subreddits with more than 1000 posts.
    - Sorts and limits results to top 10 for both most and least swearing subreddits.
    - Writes results to CSV files in HDFS.

    Note: All transformations and actions are Catalyst-optimized.
    - The use of array functions for counting swear words allows Spark to efficiently process the data in a distributed manner,
    leveraging Catalyst's optimizations for array operations.
    - Grouping, filtering, and sorting operations are also optimized by Catalyst
    """    
    # Create a Spark array column from the list of swear words for efficient intersection
    swear_words_array = F.array([F.lit(word) for word in swear_words])
    
    # Add a column counting the number of swear words in each post using array_intersect and size functions, which are optimized by Catalyst
    df = df.withColumn(
        "swear_word_count",
        F.size(F.array_intersect(F.col("content_tokens"), swear_words_array))
    )

    # Define aggregation expressions for average swear words per post and total post count
    agg_exprs = [
        F.avg("swear_word_count").alias("avg_swear_words_per_post"),
        F.count("*").alias("post_count")
    ]

    # Group by subreddit, aggregate, filter, sort, and limit results. Catalyst optimizes the entire query plan for these operations.
    most_swearing = (
        df.groupBy("subreddit")
        .agg(*agg_exprs)
        .filter(F.col("post_count") > 1000)
        .orderBy(F.col("avg_swear_words_per_post").desc())
        .limit(10)
    )
    # Write the most swearing subreddits to HDFS as CSV.
    most_swearing.coalesce(1).write.mode("overwrite").csv("/data/results/most_swearing", header=True)

    least_swearing = (
        df.groupBy("subreddit")
        .agg(*agg_exprs)
        .filter(F.col("post_count") > 1000)
        .orderBy(F.col("avg_swear_words_per_post").asc())
        .limit(10)
    )
    least_swearing.coalesce(1).write.mode("overwrite").csv("/data/results/least_swearing", header=True)

def main():
    print("[ANALYSIS] Initializing SparkSession...")
    spark = SparkSession.builder.appName("Reddit_Full_Analysis_Job").getOrCreate()

    print("[ANALYSIS] Reading cleaned Parquet data from HDFS...")
    df = spark.read.parquet("/data/cleaned_reddit/")

    swear_words = load_swear_words()
    reddit_analysis(df, swear_words)

if __name__ == "__main__":
    main()