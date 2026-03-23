from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)

def load():
    spark = SparkSession.builder \
    .appName("Load") \
    .config("spark.jars", "/mnt/d/data_engineering/Projects/e-commerce/data-eng-project/jars/postgresql-42.7.9.jar") \
    .getOrCreate()

    logging.info("Reading transformed data...")

    df = spark.read.parquet("data/intermediate/transformed")

    logging.info("Loading into PostgreSQL...")

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://192.168.31.16:5432/ecommerce") \
        .option("dbtable", "customers") \
        .option("user", "postgres") \
        .option("password", "Mayur0318") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    load()
