import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]


import sys
import os
import logging

# Fix path for config import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql.functions import col
from config import *

logging.basicConfig(level=logging.INFO)


def run_spark_etl():
    from pyspark.sql import SparkSession
    logging.info("Starting Spark ETL job...")

    try:
        spark = SparkSession.builder \
            .appName("Ecommerce ETL") \
            .config("spark.jars", JAR_PATH) \
            .getOrCreate()

        # -------------------------
        # LOAD DATA
        # -------------------------
        customers = spark.read.csv(
            "file:///mnt/d/data_engineering/Projects/e-commerce/data-eng-project/data/raw/olist_customers_dataset.csv",
            header=True,
            inferSchema=True
        )

        orders = spark.read.csv(
            "file:///mnt/d/data_engineering/Projects/e-commerce/data-eng-project/data/raw/olist_orders_dataset.csv",
            header=True,
            inferSchema=True
        )

        order_items = spark.read.csv(
            "file:///mnt/d/data_engineering/Projects/e-commerce/data-eng-project/data/raw/olist_order_items_dataset.csv",
            header=True,
            inferSchema=True
        )

        logging.info("Data loaded")

        # -------------------------
        # CLEANING
        # -------------------------
        orders_clean = orders.dropna()
        order_items_clean = order_items.dropna()
        customers_clean = customers.dropDuplicates()

        logging.info("Data cleaned")

        # -------------------------
        # TRANSFORMATION
        # -------------------------
        sales_data = orders_clean.join(order_items_clean, "order_id")

        final_df = sales_data.select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("price"),
            col("order_status")
        )

        logging.info("Data transformed")

        final_df.show(10)
        final_df.printSchema()

        # -------------------------
        # WRITE TO POSTGRES
        # -------------------------
        properties = {
            "user": DB_USER,
            "password": DB_PASSWORD,
            "driver": DB_DRIVER
        }

        final_df.write.jdbc(
            url=DB_URL,
            table="sales_data",
            mode="overwrite",
            properties=properties
        )

        logging.info("Data written to PostgreSQL successfully")

        spark.stop()

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        raise
