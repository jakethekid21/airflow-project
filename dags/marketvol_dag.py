from datetime import datetime, timedelta, date
import os
import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def download_stock(symbol, **context):
    ds = context["ds"]
    output_dir = f"/tmp/data/{ds}"
    os.makedirs(output_dir, exist_ok=True)

    start_date = date.today()
    end_date = start_date + timedelta(days=1)

    df = yf.download(symbol, start=start_date, end=end_date, interval="1m")

    file_path = os.path.join(output_dir, f"{symbol}.csv")
    df.to_csv(file_path)


def run_query(**context):
    ds = context["ds"]
    data_dir = "/tmp/marketvol"
    aapl_file = os.path.join(data_dir, "AAPL.csv")
    tsla_file = os.path.join(data_dir, "TSLA.csv")

    aapl_df = pd.read_csv(aapl_file)
    tsla_df = pd.read_csv(tsla_file)

    for df in [aapl_df, tsla_df]:
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df["High"] = pd.to_numeric(df["High"], errors="coerce")
        df["Low"] = pd.to_numeric(df["Low"], errors="coerce")
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

    aapl_df["symbol"] = "AAPL"
    tsla_df["symbol"] = "TSLA"

    combined = pd.concat([aapl_df, tsla_df], ignore_index=True)

    summary = combined.groupby("symbol").agg(
        avg_close=("Close", "mean"),
        max_high=("High", "max"),
        min_low=("Low", "min"),
        total_volume=("Volume", "sum"),
    ).reset_index()

    summary_file = os.path.join(data_dir, f"summary_{ds}.csv")
    summary.to_csv(summary_file, index=False)


with DAG(
    dag_id="marketvol",
    default_args=default_args,
    description="Airflow mini project for stock market volume pipeline",
    start_date=datetime(2026, 4, 20, 18, 0),
    schedule="0 18 * * 1-5",
    catchup=False,
) as dag:

    t0 = BashOperator(
        task_id="t0",
        bash_command="mkdir -p /tmp/data/{{ ds }} /tmp/marketvol"
    )

    t1 = PythonOperator(
        task_id="t1",
        python_callable=download_stock,
        op_kwargs={"symbol": "AAPL"},
    )

    t2 = PythonOperator(
        task_id="t2",
        python_callable=download_stock,
        op_kwargs={"symbol": "TSLA"},
    )

    t3 = BashOperator(
        task_id="t3",
        bash_command="mv /tmp/data/{{ ds }}/AAPL.csv /tmp/marketvol/AAPL.csv"
    )

    t4 = BashOperator(
        task_id="t4",
        bash_command="mv /tmp/data/{{ ds }}/TSLA.csv /tmp/marketvol/TSLA.csv"
    )

    t5 = PythonOperator(
        task_id="t5",
        python_callable=run_query,
    )

    t0 >> [t1, t2]
    t1 >> t3
    t2 >> t4
    [t3, t4] >> t5