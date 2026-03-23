from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)

def transform():
    spark = SparkSession.builder.appName("Transform").getOrCreate()

    logging.info("Reading extracted data...")

    df = spark.read.parquet("data/intermediate/extracted")

    logging.info("Cleaning data...")

    df = df.dropDuplicates()

    logging.info("Writing transformed data...")

    df.write.mode("overwrite").parquet("data/intermediate/transformed")

if __name__ == "__main__":
    transform()
