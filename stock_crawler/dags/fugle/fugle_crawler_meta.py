from datetime import datetime, timedelta, date
import pendulum
import time
import requests
from elasticsearch import Elasticsearch
import json
import os

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.db import provide_session
from airflow.models import XCom
def cleanup_xcom(context, session=None):
	dag_id = context['ti']['dag_id']
	session.query(XCom).filter(XCom.dag_id == dag_id).delete()

local_tz = pendulum.timezone("Asia/Taipei")
args = {
	"owner":"Josix",
	#"retries":3,
	#"retry_delay":timedelta(seconds=10),
}

def get_meta_func(r,no,ti):
	#r = requests.get('https://api.fugle.tw/realtime/v0.2/intraday/meta?symbolId=2330&apiToken=706707e3df7e8e54a6932b59c85b77ca')
	r = r.json()
	r = r['data']['meta']
	pop_attributes = ["isIndex","nameZhTw","industryZhTw","canDayBuySell","canDaySellBuy","canShortMargin","canShortLend","volumePerUnit","currency","isWarrant","typeZhTw","isUnusuallyRecommended"]
	for pop_attribute in pop_attributes:
		r.pop(pop_attribute)
		
	today = date.today()
	r.update({"date":today.strftime("%Y-%m-%d"),"stock":no})
	ti.xcom_push(key='meta_'+no, value=r)

def post_meta_ES_func(no,ti):
	es = Elasticsearch(hosts='127.0.0.1', port=9200)
	data = ti.xcom_pull(key='meta_'+no)
	data = json.dumps(data)
	#requests.post('127.0.0.1:9200',headers={'Content-Type':'application/json'},data = data)
	es.index(index='meta', body=data)

with DAG(
    'fugle_meta',
	default_args=args,
	start_date=datetime(2021, 5, 25, tzinfo=local_tz),
	schedule_interval="* * * * 1-5",#MON to FRI midnight in morning
    description='Fugle Meta API DAG',
    tags=['meta', 'fugle'],
	on_success_callback=cleanup_xcom
) as dag_meta:
	stocks = ['2330']
	for stock in stocks:
		r = requests.get('https://api.fugle.tw/realtime/v0.2/intraday/meta?symbolId=2330&apiToken=706707e3df7e8e54a6932b59c85b77ca')
		get_meta = PythonOperator(
			task_id='get_meta_'+stock,
			python_callable=get_meta_func,
			op_kwargs={'r': r,'no':stock}
		)
		post_meta_ES = PythonOperator(
				task_id='post_meta_ES_'+stock,
			python_callable=post_meta_ES_func,
			op_kwargs={'no': stock}
			)
		'''
		post_meta_ES = SimpleHttpOperator(
			task_id='post_op',
			http_conn_id='127.0.0.1:9200',
			endpoint='meta/_doc',
			data=json.dumps("{{task_instance.xcom_pull(key='meta_'+stock)}}"),
			headers={"Content-Type":"application/json"},
		)
		'''
		get_meta >> post_meta_ES
