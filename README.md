# Orchestrated ETL Pipeline with Apache Airflow

## Overview
This project demonstrates a production-grade ETL (Extract, Transform, Load) pipeline built using Apache Airflow. It ingests NYC TLC taxi trip data (Parquet), validates and transforms it, and loads aggregated results into a PostgreSQL data warehouse.

The pipeline is containerized using Docker and designed to handle real-world data challenges such as schema variability, large datasets, and fault tolerance.

---

## Architecture

Airflow Scheduler  
↓  
Extract (NYC TLC Parquet Data)  
↓  
Validate (Data Quality Checks)  
↓  
Transform (Cleaning + Aggregation)  
↓  
Load (PostgreSQL Data Warehouse)  

---

## Tech Stack

- Apache Airflow (2.8)
- Python (Pandas, PyArrow, SQLAlchemy)
- PostgreSQL
- Docker & Docker Compose

---

## Pipeline Details

### 1. Extract
- Downloads NYC taxi dataset in Parquet format
- Stores raw data in Airflow container

### 2. Validate
- Ensures required columns exist
- Checks for data consistency (positive fares, valid rows)
- Prevents corrupt data from entering pipeline

### 3. Transform
- Cleans invalid records
- Handles missing columns dynamically
- Performs daily aggregation:
  - Total trips
  - Revenue
  - Average fare
  - Average distance

### 4. Load
- Loads processed data into PostgreSQL
- Verifies row count for data integrity

---

## Key Features

- DAG-based workflow orchestration
- Fault-tolerant retry mechanism
- Schema-aware ingestion using PyArrow
- Memory-efficient large file processing
- Data validation layer
- End-to-end Dockerized setup

---

## How to Run

### 1. Start services
```bash
docker-compose up -d
```

### 2. Open Airflow UI
http://localhost:8080

Username: admin  
Password: admin  

### 3. Trigger DAG
- Enable DAG: `nyc_taxi_daily_etl`
- Click "Play" to run pipeline

---

## Sample Query

```sql
SELECT * FROM fact_daily_trips LIMIT 10;
```

---

## Project Structure

```
airflow-taxi-etl/
│
├── dags/
│   └── nyc_taxi_daily_etl.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── images/
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Resume Summary

Designed and implemented a fault-tolerant ETL pipeline using Apache Airflow, processing large-scale NYC taxi datasets with schema validation, transformation, and PostgreSQL integration.

---

## Future Improvements

- Add S3 for data storage
- Implement incremental loading
- Add Slack/email alerting
- Deploy on cloud (AWS/GCP)

---

## Author

Annjan Arora
