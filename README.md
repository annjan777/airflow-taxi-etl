# 🚀 NYC Taxi ETL Pipeline using Apache Airflow

## 📌 Overview

This project demonstrates a **production-grade ETL pipeline** built using **Apache Airflow**, **Docker**, and **PostgreSQL**.

The pipeline processes **3M+ NYC TLC taxi records**, running through a fully orchestrated 4-stage DAG with data quality validation, daily aggregation, and PostgreSQL loading.

- Extracts NYC TLC taxi data (Parquet) from the official TLC CloudFront endpoint
- Validates data quality using schema checks and integrity rules
- Transforms and aggregates into daily KPI metrics
- Loads into a PostgreSQL data warehouse with row-count verification

---

## 🧠 Architecture

Processing **3,066,766 records** through a 4-stage Airflow DAG:

```
Airflow Scheduler (daily @ 2AM)
        ↓
   [Extract]  — Downloads Parquet from NYC TLC (streaming, 1MB chunks)
        ↓
   [Validate] — Schema checks, fare integrity, null validation
        ↓
   [Transform]— Cleans outliers, aggregates to daily metrics
        ↓
   [Load]     — Inserts into PostgreSQL fact_daily_trips, verifies row count
```

---

## ⚙️ Tech Stack

- Apache Airflow 2.8
- Python (Pandas, PyArrow, SQLAlchemy)
- PostgreSQL 15
- Docker & Docker Compose

---

## 🔄 Pipeline Tasks

### 1. Extract
- Downloads NYC TLC Yellow Taxi dataset (Jan 2023) in Parquet format
- Streams download in 1MB chunks to handle large file size
- Validates file size on completion

### 2. Validate
- Checks required columns exist (`fare_amount`, `tpep_pickup_datetime`, `trip_distance`)
- Verifies >50% of fares are positive (structural sanity check)
- Logs negative fare count as warning — filtered in transform

### 3. Transform
- Uses PyArrow to read schema safely without loading full dataset
- Filters: `fare_amount > 0`, `trip_distance` between 0.1–200 miles
- Aggregates to daily metrics: total trips, gross revenue, avg fare, avg distance
- Converts trip_date to string for Postgres compatibility

### 4. Load
- Loads aggregated daily records into PostgreSQL `fact_daily_trips`
- Uses `if_exists="replace"` for idempotent re-runs
- Verifies row count post-insert — raises error on mismatch

---

## ✅ Pipeline Success

### DAG Graph (All 4 Tasks Successful)

![Airflow DAG Graph](images/dag_graph.png)

### DAG Run History

![Airflow DAG Runs](images/dag_runs.png)

### Task Logs

![Airflow Task Logs](images/task_logs.png)

---

## 📊 Sample Output (PostgreSQL)

![Postgres Output](images/postgres_output.png)

Real data from `fact_daily_trips` table:

| trip_date  | total_trips | gross_revenue | avg_fare | avg_distance |
|------------|-------------|---------------|----------|--------------|
| 2023-01-01 | 74,216      | 2,296,235.90  | 22.06    | 4.42         |
| 2023-01-02 | 63,758      | 2,002,015.73  | 22.25    | 4.63         |

---

## 🚀 How to Run

### 1. Clone the repo
```bash
git clone https://github.com/annjan777/airflow-taxi-etl.git
cd airflow-taxi-etl
```

### 2. Start all services
```bash
docker-compose up -d
```

### 3. Open Airflow UI
```
http://localhost:8080
Username: admin
Password: admin
```

### 4. Trigger the DAG
- Enable DAG: `nyc_taxi_daily_etl`
- Click ▶ to trigger a manual run
- All 4 tasks should turn green

---

## 📝 Sample Query

```sql
SELECT * FROM fact_daily_trips ORDER BY trip_date LIMIT 10;
```

---

## 📁 Project Structure

```
airflow-taxi-etl/
├── dags/
│   └── nyc_taxi_daily_etl.py
├── data/
│   ├── raw/
│   └── processed/
├── images/
│   ├── dag_graph.png
│   ├── dag_runs.png
│   ├── task_logs.png
│   └── postgres_output.png
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 🔮 Future Improvements

- Add AWS S3 for raw data storage
- Implement incremental loading (process only new data)
- Add Slack/email alerting on failure
- Deploy on cloud (AWS MWAA / GCP Cloud Composer)

---

## 👨‍💻 Author

Annjan 