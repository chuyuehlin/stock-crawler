from datetime import datetime, timedelta

from airflow import DAG

default_args = {
    "owner": "Josix",
    "start_date": datetime(2021, 5, 1, 0, 0),
    "schedule_interval": "@daily",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    'fugle_meta',
    default_args=default_args,
    description='Fugle Meta API DAG',
    tags=['meta', 'fugle'],
)

with dag:
    pass