import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count, round as _round

def process_ecommerce_data(spark, source_path, destination_path):
    raw_df = spark.read.format("csv") \
        .option("header", "true") \
        .load(f"{source_path}/transactions/")

    silver_df = raw_df.dropDuplicates() \
        .dropna(subset=["transaction_id", "customer_id", "amount"]) \
        .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
        .withColumn("amount", col("amount").cast("double")) \
        .filter(col("amount") > 0)

    gold_daily_revenue_df = silver_df.groupBy("transaction_date") \
        .agg(
            _round(_sum("amount"), 2).alias("daily_revenue"),
            count("transaction_id").alias("total_orders")
        ) \
        .orderBy("transaction_date")

    gold_daily_revenue_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{destination_path}/gold/daily_revenue")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ECommerce_Medallion_Architecture") \
        .getOrCreate()
    
    BRONZE_PATH = "abfss://data@yourstorageaccount.dfs.core.windows.net/raw"
    GOLD_PATH = "abfss://data@yourstorageaccount.dfs.core.windows.net/clean"
    
    process_ecommerce_data(spark, BRONZE_PATH, GOLD_PATH)
