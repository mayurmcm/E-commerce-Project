import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import SparkSession
import os

# import config file
from config import *

def run_spark_etl():

    # Fix Java path
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

    # Create Spark session (WITH PostgreSQL JAR)
    spark = SparkSession.builder \
        .appName("ETL Job") \
        .master("local[*]") \
        .config("spark.jars", JAR_PATH) \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    print("✅ Spark started successfully")

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

    print("✅ All datasets loaded")

    # -------------------------
    # TRANSFORMATION
    # -------------------------

    sales = orders.join(order_items, "order_id")

    sales_selected = sales.select(
        "order_id",
        "customer_id",
        "product_id",
        "price"
    )

    final_df = sales_selected.join(customers, "customer_id")

    print("✅ Transformation completed")

    final_df.show(5)

    print(f"✅ Total records: {final_df.count()}")

    # -------------------------
    # WRITE TO POSTGRESQL
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

    print("✅ Data written to PostgreSQL successfully")

    spark.stop()


# -------------------------
# MAIN ENTRY
# -------------------------
if __name__ == "__main__":
    run_spark_etl()