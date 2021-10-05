from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import datetime

def data_cleaning(*args,**kwargs):
    source = kwargs.get("source")
    
    list_files = []
    if(os.path.exists(source)):
    	print('I am in')
    else:
        print('I am not in')
    for files in list_files:
        if files[-3:].lower() not in ['mp3','wma','m4a','mp4','mkv']:
            print(files)

with DAG(
    dag_id="Dummy_DAG",
    description='DAG to check file operations',
    default_args={
        "owner": "airflow",
        "retries": 0,
        "start_date": datetime(2021, 1, 1),
    },
    catchup=False
) as f:
    data_cleaning = PythonOperator(
    task_id='data_cleaning',
    python_callable=data_cleaning,
    op_kwargs={
    	"source": "/mnt/d/Songs/complete songs/"
    })
