#!/usr/bin/env python
# coding: utf-8

# ## Importing Libraries

# In[4]:


try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from datetime import datetime
    import os
    import datetime
    import eyed3
    import shutil
    import mysql.connector
    print("All DAG modules are ok ......")
except Exception as e:
    print(f'{e}')


# In[ ]:


with DAG(
    dag_id="Music_Renaming_DAG_scripted",
    description='DAG to fix the music metadata through SQL load and search via Bash Scripts',
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime.datetime(2021, 1, 1),
    },
    catchup=False
) as f:
    
    bring_files_to_parent_dir = BashOperator(
    task_id='bring_files_to_parent_dir',
    bash_command='sh /home/akshit/python-code/bring_files_to_main_directory.sh')
    
    remove_numbers = BashOperator(
    task_id='remove_numbers',
    bash_command='python /home/akshit/python-code/remove_digits.py')
    
    data_cleaning = BashOperator(
    task_id='data_cleaning',
    bash_command='python /home/akshit/python-code/clean_music_data.py')
    
    write_metadata_to_file = BashOperator(
    task_id='write_metadata_to_file',
    bash_command='python /home/akshit/python-code/write_metadata_to_file.py')
    
#     write_metadata_to_db = PythonOperator(
#     task_id='write_metadata_to_db',
#     python_callable=write_metadata_to_db,
#     op_kwargs={"source":"/mnt/d/Songs/complete songs"})
    
    bring_files_to_parent_dir >> data_cleaning >> remove_numbers >> write_metadata_to_file 
#         >> write_metadata_to_db

