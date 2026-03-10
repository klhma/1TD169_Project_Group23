import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    # 1. Initialize Production SparkSession (No findspark needed for spark-submit)
    print("[ANALYSIS] Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName("Reddit_Full_Analysis_Job") \
        .getOrCreate()

    # 2. Read the cleaned Parquet data
    print("[ANALYSIS] Reading cleaned Parquet data from HDFS...")
    df = spark.read.parquet("/data/cleaned_reddit/")

    # ==========================================
    # PART 1: GENERAL REDDIT ANALYSIS
    # ==========================================
    
    # Task 1: Top 10 hottest subreddits
    print("[ANALYSIS] Calculating Top 10 hottest subreddits...")
    top_subreddits = df.groupBy("subreddit") \
                       .count() \
                       .orderBy(F.col("count").desc()) \
                       .limit(10)
    top_subreddits.coalesce(1).write.mode("overwrite").csv("/data/results/top_subreddits", header=True)

    # Task 2: Top 10 Most Active Authors (excluding deleted)
    print("[ANALYSIS] Calculating Top 10 Most Active Authors...")
    active_authors = df.filter(df.author != "[deleted]") \
                       .groupBy("author") \
                       .count() \
                       .orderBy(F.col("count").desc()) \
                       .limit(10)
    active_authors.coalesce(1).write.mode("overwrite").csv("/data/results/active_authors", header=True)

    # Task 3: Keyword Trend - 'Help/Advice' requests
    print("[ANALYSIS] Extracting Help/Advice requests...")
    keyword_regex = "(?i)(need help|please help|need advice)"
    help_requests = df.filter(F.col("body").rlike(keyword_regex)) \
                      .groupBy("subreddit") \
                      .count() \
                      .withColumnRenamed("count", "help_post_count") \
                      .orderBy(F.col("help_post_count").desc()) \
                      .limit(10)
    help_requests.coalesce(1).write.mode("overwrite").csv("/data/results/help_requests", header=True)

    # Task 4: Distributed Word Count on Summaries
    print("[ANALYSIS] Running Distributed Word Count on summaries...")
    words_df = df.select(F.explode(F.split(F.lower(F.col("summary")), "[^a-zA-Z]+")).alias("word"))
    top_words = words_df.filter(F.length(F.col("word")) > 4) \
                        .groupBy("word") \
                        .count() \
                        .orderBy(F.col("count").desc()) \
                        .limit(15)
    top_words.coalesce(1).write.mode("overwrite").csv("/data/results/top_words", header=True)

    # ==========================================
    # PART 2: LEAGUE OF LEGENDS DEEP DIVE
    # ==========================================
    print("[ANALYSIS] Filtering for 'leagueoflegends' subreddit...")
    lol_df = df.filter(F.col("subreddit") == "leagueoflegends")
    lol_df.cache() # Cache this small subset to speed up the next queries

    # Task 5: Nerf vs Buff Discussion Lengths
    print("[ANALYSIS] Analyzing 'Nerf' vs 'Buff' lengths...")
    balance_df = lol_df.withColumn("mentions_nerf", F.col("body").rlike("(?i)\\bnerf\\b|\\bnerfs\\b")) \
                       .withColumn("mentions_buff", F.col("body").rlike("(?i)\\bbuff\\b|\\bbuffs\\b"))
    
    balance_stats = balance_df.groupBy("mentions_nerf", "mentions_buff") \
                              .agg(
                                  F.count("*").alias("total_posts"),
                                  F.avg("content_len").cast("int").alias("avg_post_length")
                              ) \
                              .orderBy(F.col("total_posts").desc())
    balance_stats.coalesce(1).write.mode("overwrite").csv("/data/results/lol_nerf_vs_buff", header=True)

    # Task 6: Esports vs Casual Play
    print("[ANALYSIS] Categorizing Esports vs Casual Play...")
    esports_keywords = "(?i)\\b(lcs|lec|lck|lpl|worlds|msi|faker|t1|fnatic|g2)\\b"
    casual_keywords = "(?i)\\b(soloq|elo|ranked|lp|smurf|climbing)\\b"

    categorized_lol_df = lol_df.withColumn(
        "discussion_type",
        F.when(F.col("body").rlike(esports_keywords) & F.col("body").rlike(casual_keywords), "Both")
         .when(F.col("body").rlike(esports_keywords), "Esports / Pro Play")
         .when(F.col("body").rlike(casual_keywords), "Ranked / Casual Play")
         .otherwise("General / Other")
    )

    category_counts = categorized_lol_df.groupBy("discussion_type") \
                                        .count() \
                                        .orderBy(F.col("count").desc())
    category_counts.coalesce(1).write.mode("overwrite").csv("/data/results/lol_esports_vs_casual", header=True)

    print("[ANALYSIS] All tasks completed successfully! Results saved to /data/results/")
    spark.stop()

if __name__ == "__main__":
    main()