from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)

def extract():
    spark = SparkSession.builder.appName("Extract").getOrCreate()

    logging.info("Reading CSV...")

    df = spark.read.csv(
        "data/raw/olist_customers_dataset.csv",
        header=True,
        inferSchema=True
    )

    logging.info("Writing extracted data...")

    df.write.mode("overwrite").parquet("data/intermediate/extracted")

if __name__ == "__main__":
    extract()
