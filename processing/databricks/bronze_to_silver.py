from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def process_bronze_to_silver():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    
    # Read Bronze
    df = spark.read.format("json").load("gs://my-bucket-bronze/events/")
    
    # Cleansing
    cleaned_df = df.filter(col("id").isNotNull()).withColumn("processed_at", current_timestamp())
    
    # Write Silver (Delta)
    cleaned_df.write.format("delta").mode("append").save("gs://my-bucket-silver/events/")

if __name__ == "__main__":
    process_bronze_to_silver()
