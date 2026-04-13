from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import requests
import os
from sqlalchemy import create_engine, text

default_args = {
    "owner": "annjan",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "depends_on_past": False,
}

with DAG(
    dag_id="nyc_taxi_daily_etl",
    default_args=default_args,
    description="Robust NYC TLC ETL pipeline",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "taxi", "production"],
) as dag:

    # =========================
    # EXTRACT
    # =========================
    def extract_taxi_data(**context):
        url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
        output_path = "/opt/airflow/data/raw/data.parquet"
        os.makedirs("/opt/airflow/data/raw", exist_ok=True)

        print(f"Downloading from: {url}")
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"Downloaded {size_mb:.1f} MB")

        if size_mb < 1:
            raise ValueError("Downloaded file too small")

        context["ti"].xcom_push(key="raw_file", value=output_path)
        print("Extract complete")

    # =========================
    # VALIDATE
    # =========================
    def validate_data(**context):
        raw_file = context["ti"].xcom_pull(key="raw_file")

        df = pd.read_parquet(raw_file, columns=[
            "tpep_pickup_datetime",
            "fare_amount",
            "trip_distance",
        ])

        print(f"Rows: {len(df):,}")

        if len(df) == 0:
            raise ValueError("FAILED: file has zero rows")

        required = ["tpep_pickup_datetime", "fare_amount", "trip_distance"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"FAILED: missing columns {missing}")

        fare = pd.to_numeric(df["fare_amount"], errors="coerce").fillna(0)
        pct_positive = (fare > 0).mean()
        if pct_positive < 0.5:
            raise ValueError(f"FAILED: only {pct_positive:.1%} fares positive — data corrupt")

        neg_count = (fare < 0).sum()
        print(f"Negative fares: {neg_count:,} — will be filtered in transform")
        print(f"Validation passed. {pct_positive:.1%} positive fares.")
        context["ti"].xcom_push(key="row_count", value=len(df))

    # =========================
    # TRANSFORM
    # FIX: use pyarrow to get schema — works in all pandas versions
    # =========================
    def transform_data(**context):
        raw_file = context["ti"].xcom_pull(key="raw_file")

        # FIX: use pyarrow to safely read column names without loading data
        import pyarrow.parquet as pq
        schema = pq.read_schema(raw_file)
        available = schema.names
        print(f"Columns in file: {available}")

        cols_needed = [
            "tpep_pickup_datetime",
            "fare_amount",
            "total_amount",
            "trip_distance",
        ]
        cols_to_read = [c for c in cols_needed if c in available]
        print(f"Reading: {cols_to_read}")

        if "fare_amount" not in cols_to_read:
            raise ValueError(f"fare_amount not found in parquet. Available: {available}")

        df = pd.read_parquet(raw_file, columns=cols_to_read)
        print(f"Raw rows: {len(df):,}")

        # Add missing optional columns
        if "total_amount" not in df.columns:
            df["total_amount"] = 0.0
        if "trip_distance" not in df.columns:
            df["trip_distance"] = 0.0

        # Convert to numeric
        df["fare_amount"]   = pd.to_numeric(df["fare_amount"],   errors="coerce").fillna(0)
        df["total_amount"]  = pd.to_numeric(df["total_amount"],  errors="coerce").fillna(0)
        df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce").fillna(0)

        # Filter bad rows
        before = len(df)
        df = df[df["fare_amount"] > 0]
        df = df[df["trip_distance"].between(0.1, 200)]
        print(f"After filtering: {len(df):,} rows (removed {before - len(df):,})")

        # Parse datetime
        if not pd.api.types.is_datetime64_any_dtype(df["tpep_pickup_datetime"]):
            df["tpep_pickup_datetime"] = pd.to_datetime(
                df["tpep_pickup_datetime"], errors="coerce"
            )
        df = df.dropna(subset=["tpep_pickup_datetime"])
        df["trip_date"] = df["tpep_pickup_datetime"].dt.date

        print(f"Rows after datetime clean: {len(df):,}")

        # Daily aggregation
        daily_agg = df.groupby("trip_date").agg(
            total_trips   = ("fare_amount",   "count"),
            gross_revenue = ("total_amount",  "sum"),
            avg_fare      = ("fare_amount",   "mean"),
            avg_distance  = ("trip_distance", "mean"),
        ).reset_index()

        daily_agg["gross_revenue"] = daily_agg["gross_revenue"].round(2)
        daily_agg["avg_fare"]      = daily_agg["avg_fare"].round(2)
        daily_agg["avg_distance"]  = daily_agg["avg_distance"].round(2)
        daily_agg["trip_date"]     = daily_agg["trip_date"].astype(str)

        os.makedirs("/opt/airflow/data/processed", exist_ok=True)
        output_path = "/opt/airflow/data/processed/output.csv"
        daily_agg.to_csv(output_path, index=False)

        print(f"Transform complete — {len(daily_agg)} daily records")
        print(daily_agg.to_string())
        context["ti"].xcom_push(key="processed_file", value=output_path)

    # =========================
    # LOAD
    # =========================
    def load_to_postgres(**context):
        processed_file = context["ti"].xcom_pull(key="processed_file")
        df = pd.read_csv(processed_file)

        print(f"Loading {len(df)} rows to PostgreSQL...")
        print(df.head(5).to_string())

        engine = create_engine(
            "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
        )

        df.to_sql(
            "fact_daily_trips",
            engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=500,
        )

        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM fact_daily_trips")
            ).scalar()

        if count != len(df):
            raise ValueError(f"Load mismatch: expected {len(df)}, got {count} in DB")

        print(f"Load complete — {count} rows in fact_daily_trips")

    # =========================
    # WIRE UP
    # =========================
    t_extract   = PythonOperator(task_id="extract",   python_callable=extract_taxi_data)
    t_validate  = PythonOperator(task_id="validate",  python_callable=validate_data)
    t_transform = PythonOperator(task_id="transform", python_callable=transform_data)
    t_load      = PythonOperator(task_id="load",      python_callable=load_to_postgres)

    t_extract >> t_validate >> t_transform >> t_load