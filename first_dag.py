#!/usr/bin/env python
# coding: utf-8

# In[3]:


try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))
    
    

    
def hello_world():
    print("Hello World")
    return "Hello world"


with DAG(
        dag_id="first_dag",
    description="Hello world of DAG"
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:
     hello_world = PythonOperator(
        task_id="hello_world",
        python_callable=hello_world,
        provide_context=True,
        op_kwargs={"name":"Akshit Thakur"}
    )


# In[ ]:




