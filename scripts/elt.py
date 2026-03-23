import pandas as pd
import psycopg2

# -----------------------------
# DB CONNECTION
# -----------------------------
conn = psycopg2.connect(
    dbname="ecommerce",
    user="postgres",
    password="Mayur0318",   # <-- update this
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

print("✅ Connected to PostgreSQL")

# -----------------------------
# LOAD CSV FILES
# -----------------------------
customers = pd.read_csv("../data/raw/olist_customers_dataset.csv")
orders = pd.read_csv("../data/raw/olist_orders_dataset.csv")
order_items = pd.read_csv("../data/raw/olist_order_items_dataset.csv")

print("✅ CSV files loaded")

# -----------------------------
# CLEAN DATA
# -----------------------------
customers.drop_duplicates(inplace=True)
orders.dropna(inplace=True)
order_items.dropna(inplace=True)

print("✅ Data cleaned")

# -----------------------------
# CREATE TABLES
# -----------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    customer_city TEXT,
    customer_state TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS order_items (
    order_id TEXT,
    product_id TEXT,
    price FLOAT
);
""")

conn.commit()
print("✅ Tables created")

# -----------------------------
# INSERT DATA
# -----------------------------

# Customers
for _, row in customers.iterrows():
    cursor.execute("""
    INSERT INTO customers VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING;
    """, (row['customer_id'], row['customer_city'], row['customer_state']))

# Orders
for _, row in orders.iterrows():
    cursor.execute("""
    INSERT INTO orders VALUES (%s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """, (row['order_id'], row['customer_id'], row['order_status'], row['order_purchase_timestamp']))

# Order Items
for _, row in order_items.iterrows():
    cursor.execute("""
    INSERT INTO order_items VALUES (%s, %s, %s);
    """, (row['order_id'], row['product_id'], row['price']))

conn.commit()
print("✅ Data inserted successfully")

# -----------------------------
# CLOSE CONNECTION
# -----------------------------
cursor.close()
conn.close()

print("🎉 ETL Process Completed Successfully!")