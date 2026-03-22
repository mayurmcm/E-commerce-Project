import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

from config import *

logging.basicConfig(level=logging.INFO)


spark = SparkSession.builder \
    .appName("Ecommerce ETL") \
    .config("spark.jars", JAR_PATH) \
    .getOrCreate()

try:
    # -----------------------------
    # LOAD DATA
    # -----------------------------
    customers = spark.read.csv(
        "file:///D:/data_engineering/Projects/e-commerce/data-eng-project/data/raw/olist_customers_dataset.csv",
        header=True,
        inferSchema=True
    )

    orders = spark.read.csv(
        "file:///D:/data_engineering/Projects/e-commerce/data-eng-project/data/raw/olist_orders_dataset.csv",
        header=True,
        inferSchema=True
    )

    order_items = spark.read.csv(
        "file:///D:/data_engineering/Projects/e-commerce/data-eng-project/data/raw/olist_order_items_dataset.csv",
        header=True,
        inferSchema=True
    )

    logging.info("Data loaded")

    # CLEANING
    orders_clean = orders.dropna()
    order_items_clean = order_items.dropna()
    customers_clean = customers.dropDuplicates()

    logging.info("Data cleaned")

    # TRANSFORMATION
    sales_data = orders_clean.join(order_items_clean, "order_id")

    final_df = sales_data.select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("price"),
        col("order_status")
    )

    logging.info("Data transformed")

    # SHOW
    final_df.show(10)
    final_df.printSchema()
    logging.info(f"Total records: {final_df.count()}")

    # WRITE
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

except Exception as e:
    logging.error(f"Error occurred: {e}")

