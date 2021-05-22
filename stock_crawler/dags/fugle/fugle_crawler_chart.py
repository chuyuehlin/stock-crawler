from datetime import datetime, timedelta
import requests
from elasticsearch import Elasticsearch

from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
default_args = {
    "owner": "Josix",
    "start_date": datetime(2021, 5, 1, 0, 0),
	"schedule_interval"='*/1 * * * *', #..every 1 minutes
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def get_chart_func(no,ti):
	r = requests.get('https://api.fugle.tw/realtime/v0.2/intraday/chart?symbolId=') + no + ('&apiToken=706707e3df7e8e54a6932b59c85b77ca')
	r = r.json()
	r = r['data']['chart']
	ti.xcom_push(key='chart_'+no, value=r)

def post_chart_ES_func(no,ti):
	es = Elasticsearch(hosts='127.0.0.1', port=9200)
	data = ti.xcom_pull(key='chart_'+no)
	es.index(index='chart', body=data)

dag_chart = DAG(
    'fugle_chart',
    default_args=default_args,
    description='Fugle Chart API DAG',
    tags=['chart', 'fugle'],
)

with dag_chart:
	stocks = ['2330','2454']
	for stock in stocks:
		get_chart = PythonOperator(
			task_id='get_chart_'+stock,
			python_callable=get_chart_func,
			op_kwargs={'no': stock}
		)
		post_chart_ES = PythonOperator(
			task_id='post_chart_ES_'+stock,
			python_callable=post_chart_ES_func,
			op_kwargs={'no': stock}
		)
		get_chart >> post_chart_ES
