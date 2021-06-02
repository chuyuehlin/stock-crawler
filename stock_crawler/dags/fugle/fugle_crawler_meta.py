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
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.operators.http_operator import SimpleHttpOperator
local_tz = pendulum.timezone("Asia/Taipei")
args = {
	"owner":"Josix",
	"retries":2,
	"retry_delay":timedelta(seconds=60),
}

def meta_processing_func(no,ti):
	r = ti.xcom_pull(key='return_value',task_ids='get_meta_'+no)
	today = r['info']['date']
	r = r['meta']
	pop_attributes = ["isIndex","nameZhTw","industryZhTw","canDayBuySell","canDaySellBuy","canShortMargin","canShortLend","volumePerUnit","currency","isWarrant","typeZhTw","isUnusuallyRecommended"]
	for pop_attribute in pop_attributes:
		r.pop(pop_attribute)
		
	r.update({"date":today,"stock":no})
	ti.xcom_push(key='meta_'+no, value=r)

def post_meta_ES_func(no,ti):
	es = Elasticsearch(hosts='127.0.0.1', port=9200)
	data = ti.xcom_pull(key='meta_'+no)
	data = json.dumps(data)
	es.index(index='meta', body=data)

with DAG(
	'fugle_meta',
	default_args=args,
	start_date=datetime(2021, 5, 27, tzinfo=local_tz),
	schedule_interval="0 8 * * 1-5",#MON to FRI 00:05 in morning
	description='Fugle Meta API DAG',
	tags=['meta', 'fugle'],
) as dag_meta:
	stocks = ['1101', '1102', '1103', '1104', '1109','2330']
	for stock in stocks:
		get_meta = SimpleHttpOperator(
			task_id='get_meta_'+stock,
			method='GET',
			http_conn_id='fugle_API',
			endpoint='/realtime/v0.2/intraday/meta?symbolId='+stock+'&apiToken=706707e3df7e8e54a6932b59c85b77ca',
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
