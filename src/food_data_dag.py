# import packages and modules
import sys
import time

from datetime import datetime, timedelta,timezone
from typing import Optional

from airflow.decorators import dag, task
from airflow.models import TaskInstance

from airflow import models, DAG


from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator


# use updated operator if deprecated warning occcurs due airflow version
# from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# oprator defines for which language DAG will be executed
# bash operator for shell scripts and python operator for running .py files and py functions

# slack webbhook URL 
SLACK_WEBHOOK_URL = ''

def send_slack_notification(context):
    if SLACK_WEBHOOK_URL:
        failed_task = context.get("task_instance").task_id
        msg = f":red_circle: Hello {default_args['owner']} ! Your Airflow Task with Task ID *{failed_task}* has been failed in DAG *{context.get('dag_run').dag_id}*"
        
        SlackWebhookHook(
            webhook_token=SLACK_WEBHOOK_URL,
            message=msg,
        ).execute()


# create default args
default_args = {
    'owner': 'Nainesh Khanjire',# default arguments
    #'start_date': datetime(2023,7,29),
    'start_date': datetime(2023, 7, 29, tzinfo=timezone(timedelta(hours=5, minutes=30))), # for IST 
    'retries': 0, 
    'retry_delay': timedelta(seconds=50), # time gap between retries
    'dataflow_default_options': { 
        'project': 'cervelloproject',
        'region': 'us-central1', #region of compute engine
        'runner': 'DataflowRunner' # where the pipline is going to run
    }
}

dag = models.DAG(
    'food_orders_dag', # change the dag name
    default_args=default_args,
    description='DAG to monitor GCP tasks',
    schedule_interval= '0 10 * * *', 
    #schedule_interval=timedelta(days=1), # alternate schedule argument
    catchup=False,# catchup means backfilling the jobs, if start date is 1st july and DAG file is loaded today, it will execute it for all days of july
)


t1 = GCSObjectExistenceSensor( # >> task 1 to check if the file exists in storage bucket
    task_id='check_data_file_existence',
    on_failure_callback=send_slack_notification,
    bucket='foodtable', # bucket name
    object='food_daily.cs', # data file with mistake to check error
    mode='poke', # checks for the existing file
    poke_interval=60 * 1,  # check every 5 minutes
    depends_on_past = False,
    timeout=60 * 1, # timeout after 1 minute
    dag=dag) 

t2 = GCSObjectExistenceSensor( # >> task 1 to check if the python code file exists in storage bucket
    task_id='check_code_file_existence',
    on_failure_callback=send_slack_notification,
    bucket='us-east1-composer-advanced--2fff460f-bucket', # bucket name
    object='apache_beam_pipeline.py', # code file in bucket with mistake to check error
    mode='poke', # checks for the existing file
    #depends_on_past = False,
    poke_interval=60 * 1,  # check every 5 minutes
    timeout=60 * 1, # timeout after 1 minute
    dag=dag) 

t3 = DataFlowPythonOperator(
    task_id='beamtask', # DAG task, task id
    on_failure_callback=send_slack_notification,
    #depends_on_past = True,
    py_file='gs://us-east1-composer-advanced--2fff460f-bucket/apache_beam_pipeline.py', # location of python file after creating airflow environment
    options={'input' : 'gs://foodtable/food_daily.csv'}, # input file location
    dag=dag
)   

# setting on failure callback for all tasks
for i in [t1, t2, t3]:
    i.on_failure_callback = send_slack_notification
 
# defining task dependencies, t1 and t2 will run parallely after start_task 
[t1 , t2] >> t3 

