from datetime import datetime, timedelta
import requests
from elasticsearch import Elasticsearch

from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
default_args = {
    "owner": "Josix",
    "start_date": datetime(2021, 5, 1, 0, 0),
    "schedule_interval": "@daily",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def get_meta_func(no,ti):
	r = requests.get('wss://api.fugle.tw/realtime/v0.2/intraday/meta?symbolId=')+ no +('&apiToken=706707e3df7e8e54a6932b59c85b77ca')
	r = r.json()
	r = r['data']['meta']
	ti.xcom_push(key='meta_'+no, value=r)

def post_meta_ES_func(no,ti):
	es = Elasticsearch(hosts='127.0.0.1', port=9200)
	data = ti.xcom_pull(key='meta_'+no)
	es.index(index='meta', body=data)

dag_meta = DAG(
    'fugle_meta',
    default_args=default_args,
    description='Fugle Meta API DAG',
    tags=['meta', 'fugle'],
)

with dag_meta:
	stocks = ['2330','2454']
	for stock in stocks:
		get_meta = PythonOperator(
			task_id='get_meta_'+stock,
			python_callable=get_meta_func,
			op_kwargs={'no': stock}
		)
		post_meta_ES = PythonOperator(
			task_id='post_meta_ES_'+stock,
			python_callable=post_meta_ES_func,
			op_kwargs={'no': stock}
		)
		get_meta >> post_meta_ES


