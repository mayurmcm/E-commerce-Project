# Ecommerce ETL Pipeline (PySpark + PostgreSQL)

## 📌 Overview
Built an end-to-end ETL pipeline using PySpark to process e-commerce data and load it into PostgreSQL.

## ⚙️ Tech Stack
- Python
- PySpark
- PostgreSQL
- JDBC

## 🔄 Pipeline Flow
1. Extract data from CSV files
2. Clean data (remove nulls, duplicates)
3. Transform data (join orders and order_items)
4. Load data into PostgreSQL

## 📊 Data Processed
- Customers dataset
- Orders dataset
- Order items dataset

## 🚀 How to Run
```bash
python scripts/spark_etl.py