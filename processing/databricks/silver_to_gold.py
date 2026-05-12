from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

def process_silver_to_gold():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
    
    # Read Silver
    df = spark.read.format("delta").load("gs://my-bucket-silver/events/")
    
    # Aggregation
    agg_df = df.groupBy("product_id").agg(
        sum("amount").alias("total_sales"),
        count("id").alias("transaction_count")
    )
    
    # Write Gold
    agg_df.write.format("delta").mode("overwrite").save("gs://my-bucket-gold/sales_aggregate/")

if __name__ == "__main__":
    process_silver_to_gold()
