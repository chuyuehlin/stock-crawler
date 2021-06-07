from datetime import datetime, timedelta, date
import pendulum
import time
import requests
from elasticsearch import Elasticsearch
import json
import os
import csv

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.db import provide_session
from airflow.models import XCom, Variable
local_tz = pendulum.timezone("Asia/Taipei")
args = {
	"owner":"Josix",
	"retries":2,
	"retry_delay":timedelta(seconds=60),
}

def meta_processing_func(no,ti):
	r = ti.xcom_pull(key='return_value',task_ids='get_meta_'+no)
	info = r['info']
	r = r['meta']
	r.update(info)
	pop_attributes = ["lastUpdatedAt","canDayBuySell","canDaySellBuy","canShortMargin","canShortLend","volumePerUnit","currency","typeZhTw","isUnusuallyRecommended"]
	for pop_attribute in pop_attributes:
		r.pop(pop_attribute)
		
	ti.xcom_push(key='meta_'+no, value=r)

def post_meta_ES_func(no,ti):
	es = Elasticsearch(hosts=Variable.get("ES_CONNECTION"), port=9200)
	data = ti.xcom_pull(key='meta_'+no)
	data = json.dumps(data)
	es.index(index='meta', body=data)

with DAG(
	'fugle_meta',
	default_args=args,
	start_date=datetime(2021, 6, 1, tzinfo=local_tz),
	schedule_interval="21 20 * * 1-5",#MON to FRI 00:05 in morning
	description='Fugle Meta API DAG',
	tags=['meta', 'fugle'],
) as dag_meta:
	stocks = (Variable.get("ALL_STOCK_ID", deserialize_json=True))["all"] 
	for stock in stocks:
		stock=str(stock)
		get_meta = SimpleHttpOperator(
			task_id='get_meta_'+stock,
			method='GET',
			http_conn_id='fugle_API',
			endpoint='/realtime/v0.2/intraday/meta?symbolId='+stock+'&apiToken='+Variable.get("FUGLE_API_TOKEN"),
			response_filter=lambda response: response.json()['data']
		)
		meta_processing = PythonOperator(
			task_id='meta_processing_'+stock,
			python_callable=meta_processing_func,
			op_kwargs={'no':stock}
		)
		post_meta_ES = PythonOperator(
			task_id='post_meta_ES_'+stock,
			python_callable=post_meta_ES_func,
			op_kwargs={'no': stock}
		)
		get_meta >> meta_processing >> post_meta_ES
