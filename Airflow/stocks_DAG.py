from airflow import DAG
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

with DAG(
    dag_id="market_etl",
    start_date=datetime(2024, 1, 1, 9),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:
    
    # Create a task using the TaskFlow API
    @task()
    def hit_polygon_api(**context):
        # Instantiate a list of tickers that will be pulled and looped over
        stock_ticker = "AMZN"
        # Set variables
        polygon_api_key = "QmK4Xeh6DMEJ6_T1q7fXoqIfBc8vIvy_"
        ds = context.get("ds")
        ds = "2025-04-10"
        # Create the URL
        url = f"https://api.polygon.io/v1/open-close/{stock_ticker}/{ds}?adjusted=true&apiKey={polygon_api_key}"
        response = requests.get(url)
        # Return the raw data
        print(url)
        print(response.json())
        return response.json()
    
    @task
    def flatten_market_data(polygon_response, **context):
        columns = {
            "status": None,
            "from": "2025-04-10",
            "symbol": "AMZN",
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None
        }
        flattened_record = {col: polygon_response.get(col, default) for col, default in columns.items()}
        return flattened_record  # Now it's JSON-safe

    
    @task
    def load_market_data(flattened_record):
        db_path = "/home/will/airflowTestDB/market_data.db"
        db_dir = os.path.dirname(db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        market_database_hook = SqliteHook("market_database_conn")
        conn = market_database_hook.get_conn()

        df = pd.DataFrame([flattened_record])
        df.to_sql(
            name="market_data",
            con=conn,
            if_exists="append",
            index=False
        )

    # Set dependencies between tasks
    raw_market_data = hit_polygon_api()
    transformed_market_data = flatten_market_data(raw_market_data)
    load_market_data(transformed_market_data)
