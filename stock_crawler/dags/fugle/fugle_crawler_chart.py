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
def cleanup_xcom(context, session=None):
	dag_id = context['ti']['dag_id']
	session.query(XCom).filter(XCom.dag_id == dag_id).delete()

local_tz = pendulum.timezone("Asia/Taipei")
args = {
	"owner":"Josix",
	"retries":3,
	"retry_delay":timedelta(seconds=10),
}

def get_chart_func(r,no,ti):
	time_now = time.strftime("%H:%M", local)
	if time_now = '09:00' or (time_now > '13:26' and time_now != '13:31'):
		continue
	#from 9:01 to 13:26 and 13:31
	local = time.localtime()
	today = date.today()
	current = today.strftime("%Y-%m-%dT")+time.strftime("%H:", local)+"{:02d}".format(local.tm_min-1)+":00.000+08:00"
	#data will delay 1 min
	r = r.json()
	r = r['data']['chart']
	r = r[current]
	r.update({"date":today.strftime("%Y-%m-%d"),"time":time.strftime("%H:", local)+"{:02d}".format(local.tm_min-1),"stock":no})	
	ti.xcom_push(key='chart_'+no, value=r)

def post_chart_ES_func(no,ti):	
	es = Elasticsearch(hosts='127.0.0.1', port=9200)
	data = ti.xcom_pull(key='chart_'+no)
	data = json.dumps(data)
	es.index(index='chart', body=data)

with DAG(
    'fugle_chart',
	default_args=args,
	start_date=datetime(2021, 5, 25, tzinfo=local_tz),
	schedule_interval="*/1 9-14 * * 1-5",#MON to FRI midnight in morning
    description='Fugle Meta API DAG',
    tags=['chart', 'fugle'],
	on_success_callback=cleanup_xcom
) as dag_chart:
	with open('id.csv', newline='') as csvfile:
		rows = csv.reader(csvfile)
		stocks = list(rows)[0]
	
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
