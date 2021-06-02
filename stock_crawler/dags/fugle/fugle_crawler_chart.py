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

def chart_processing_func(no,ti):
	r = ti.xcom_pull(key='return_value',task_ids='get_chart_'+no)
	r = r['chart']#data for every minute 
	time_list = [ t for t in r]#build list of time(string) from 9:00 to 13:30
	push_items = []
	for time in time_list:
		item = r[time] #find out the data in the list for every time
		item.update({"time":time,"stock":no}) #record time into the data
		push_items.append(item) #save it 
	ti.xcom_push(key='chart_'+no, value=push_items) #push it the xcom

def post_chart_ES_func(no,ti):
        es = Elasticsearch(hosts=Variable.get("ES_CONNECTION"), port=9200)
        datas = ti.xcom_pull(key='chart_'+no)
        for data in datas:#insert each of data into ES
                data = json.dumps(data) 
                es.index(index='chart', body=data)

with DAG(
	'fugle_chart',
	default_args=args,
	start_date=datetime(2021, 5, 27, tzinfo=local_tz),
	schedule_interval="0 14 * * 1-5",#MON to FRI 17:00
	description='Fugle Meta API DAG',
	tags=['chart', 'fugle'],
) as dag_chart:
	stocks = (Variable.get("ALL_STOCK_ID", deserialize_json=True))["all"]
	for stock in stocks:
		stock=str(stock)
		get_chart = SimpleHttpOperator(
			task_id='get_chart_'+stock,
			method='GET',
			http_conn_id='fugle_API',
			endpoint='realtime/v0.2/intraday/chart?symbolId='+stock+'&apiToken='+Variable.get("FUGLE_API_TOKEN"),
			response_filter=lambda response: response.json()['data']
		)
		chart_processing = PythonOperator(
			task_id='chart_processing_'+stock,
			python_callable=chart_processing_func,
			op_kwargs={'no':stock}
		)
		post_chart_ES = PythonOperator(
			task_id='post_chart_ES_'+stock,
			python_callable=post_chart_ES_func,
			op_kwargs={'no': stock}
		)
		get_chart >> chart_processing >> post_chart_ES
