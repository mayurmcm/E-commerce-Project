# 🚀 End-to-End Data Engineering ETL Pipeline

### (Apache Airflow + PySpark + PostgreSQL)

---

## 📌 Project Overview

This project demonstrates a **production-style ETL (Extract → Transform → Load) data pipeline** built using:

* **Apache Airflow** for orchestration
* **PySpark** for distributed data processing
* **PostgreSQL** as the target data warehouse

The pipeline processes raw CSV data, performs transformations, and loads clean data into PostgreSQL—fully automated and scheduled via Airflow.

---

## 🏗️ Architecture

```
            +-------------------+
            |   Apache Airflow  |
            |  (Orchestration)  |
            +---------+---------+
                      |
        ---------------------------------
        |               |               |
   [Extract]       [Transform]       [Load]
        |               |               |
   Read CSV       Clean & Process    Write to DB
        |               |               |
        --------> Parquet Files <-------
                      |
              PostgreSQL Database
```

---

## 🔄 Pipeline Workflow

### 1️⃣ Extract Stage

* Reads raw dataset (CSV format)
* Uses PySpark to load data efficiently
* Stores intermediate data in **Parquet format**

### 2️⃣ Transform Stage

* Removes duplicates
* Handles missing/null values
* Applies data cleaning logic
* Writes cleaned data to intermediate storage

### 3️⃣ Load Stage

* Reads transformed data
* Loads into PostgreSQL using JDBC
* Uses optimized batch write via Spark

---

## ⚙️ Tech Stack

| Tool           | Purpose                     |
| -------------- | --------------------------- |
| Apache Airflow | Workflow orchestration      |
| PySpark        | Distributed data processing |
| PostgreSQL     | Data storage                |
| Python         | Core programming language   |
| JDBC           | Database connectivity       |
| WSL (Ubuntu)   | Execution environment       |

---

## 📁 Project Structure

```
data-eng-project/
│
├── dags/
│   └── spark_etl_dag.py        # Airflow DAG definition
│
├── scripts/
│   ├── extract.py              # Extract logic
│   ├── transform.py            # Transformation logic
│   ├── load.py                 # Load to PostgreSQL
│   └── config.py (optional)    # Config variables
│
├── data/
│   ├── raw/                    # Input CSV files
│   └── intermediate/           # Parquet outputs
│
├── jars/
│   └── postgresql-42.x.x.jar
```

