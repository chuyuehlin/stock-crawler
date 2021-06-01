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
local_tz = pendulum.timezone("Asia/Taipei")
args = {
	"owner":"Josix",
	"retries":3,
	"retry_delay":timedelta(seconds=10),
}

def get_chart_func(r,no,ti):
        r = r.json()
        r = r['data']['chart']
        time_list = [ t for t in r]
        push_items = []
        for time in time_list:
                item = r[time]
                item.update({"time":time,"stock":no})
                push_items.append(item) 
        ti.xcom_push(key='chart_'+no, value=push_items)

def post_chart_ES_func(no,ti):
        es = Elasticsearch(hosts='127.0.0.1', port=9200)
        datas = ti.xcom_pull(key='chart_'+no)
        for data in datas:
                data = json.dumps(data) 
                es.index(index='chart', body=data)

with DAG(
	'fugle_chart',
	default_args=args,
	start_date=datetime(2021, 5, 27, tzinfo=local_tz),
	schedule_interval="*/1 9-14 * * 1-5",#MON to FRI every minute from 9:00 to 14:00
	description='Fugle Meta API DAG',
	tags=['chart', 'fugle'],
) as dag_chart:
	stocks=['2330']
	for stock in stocks:
		r = requests.get('https://api.fugle.tw/realtime/v0.2/intraday/chart?symbolId='+stock+'&apiToken=706707e3df7e8e54a6932b59c85b77ca')
		get_chart = PythonOperator(
			task_id='get_chart_'+stock,
			python_callable=get_chart_func,
			op_kwargs={'r':r,'no':stock}
		)
		post_chart_ES = PythonOperator(
			task_id='post_chart_ES_'+stock,
			python_callable=post_chart_ES_func,
			op_kwargs={'no': stock}
		)
		get_chart >> post_chart_ES
