from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import json
from google.cloud import secretmanager
from google.cloud import bigquery
from google.cloud import storage
import psycopg2
import mysql.connector
# import tdf_config_backend
from pyarrow.parquet import ParquetDataset
import io
import os
import gcsfs
import csv
import google.auth.transport.requests
import google.oauth2.id_token
import pytz
#from airflow.contrib.utils.sendgrid import send_email
from datetime import datetime
import subprocess
from datetime import date
import datetime as dt
import logging
import pandas as pd
import requests
import sys
import urllib
import time
from datetime import timedelta
from airflow.api.common.experimental import get_task_instance
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
import psycopg2
from googleapiclient import discovery





project='gcp-accelerator-380712'
region='us-central1'
zone='us-central1-a'
postgres_vm_name='vm-rdbms'
postgres_host='10.128.0.49'
postgres_port=5432
postgres_database='tdf_metadata'
postgres_user='postgres'
postgres_password='Tiger#1234'
landing_zone='tdf_landing_zone'
secured_landing_zone='tdf_secured_landing_zone'
target_bucket_name='udf_target_systems'
target_db_name='test_target_db'

# Fetching External IP of a VM
# def fetchExternalIP(vm_name):
#     service = discovery.build('compute', 'v1', cache_discovery=False)
#     response = service.instances().get(project=project, zone=zone,instance=vm_name).execute()
#     ip = response['networkInterfaces'][0]['accessConfigs'][0]['natIP']
#     return ip

connection = psycopg2.connect(
    host = postgres_host,
    port = postgres_port,
    database = postgres_database,
    user = postgres_user,
    password = postgres_password
)


# service_account_path = "gs://rdbms/required_files/sandbox-gcp-322412-a44765c32528.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path
# url for publishing the data to pubsub topic
# endpoint_url = "https://us-central1-sandbox-gcp-322412.cloudfunctions.net/get_audit_data_and_publish_to_pubsub"

# url to publish the data directly to the bigquery (auditing) table
# endpoint_url = "https://us-central1-sandbox-gcp-322412.cloudfunctions.net/direct-cloudfunction-to-bigquery"
endpoint_url  = "https://us-central1-gcp-accelerator-380712.cloudfunctions.net/tdf_audit_unified"
endpoint_url_ge = "https://us-central1-gcp-accelerator-380712.cloudfunctions.net/tdf_audit_unified_helper_ge"

def make_authorized_get_request(service_url):
    """
    make_authorized_get_request makes a GET request to the specified HTTP endpoint
    in service_url (must be a complete URL) by authenticating with the
    ID token obtained from the google-auth client library.
    """
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, service_url)
    print("Id Token is " + str(id_token))

    return id_token


log = logging.getLogger("airflow.task.operators")
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
log.addHandler(handler)
#emailIds = Variable.get('support_email_ids')
'''''
#Funtion which notifies failure event through email
def notify_failure_email(**context):
    print('Email Notification Started')
    executionDate = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    ComposerLogURL = context.get('task_instance').log_url
    ComposerLogURLStr =str(ComposerLogURL)
    taskDetails = context.get('task_instance_key_str').split('__')
    job = taskDetails[0]
    FailureEnv="Development"
    DagExecutionDt= str((context['execution_date']).strftime("%Y%m%d%H%M%S"))
    print("Execution Date Val is ")
    print(DagExecutionDt)
    TaskName=str(taskDetails[1])
    print(TaskName)
    print(taskDetails)
    html_header = """Failure : {environment} : {job}.{task} is failed at {time}""".format(job=taskDetails[0], task=taskDetails[1],
                                                                          time=executionDate, environment=FailureEnv)
    html_content = """
                Hi Team,<br>
                                 The Job <font color="blue">{job}</font> has failed in task <font color="blue">{task}</font> at {time} in <font color="blue">{environment}</font>. 
                                 <br>Please check the below composer log for the root cause of failure and take necessary action to resolve this issue.<br>
                                 <br>Composer Log URL - <font color="blue">{ComposerLogURLStrVal}</font><br>
                                 <br>
                                 <br>    
                                 Thanks and Regards,<br>
                                 GCP Accelerator Composer(Airflow)
                """.format(
        job=job,
        task=taskDetails[1],
        time=executionDate,
        environment=FailureEnv,
        ComposerLogURLStrVal=ComposerLogURLStr)
    email = EmailOperator(
        task_id='send_email',
        to=emailIds,
        subject=html_header,
        html_content=html_content
    )
    email.execute(context)
    print('Email Notification Completed')
'''''

#===========================================================================================
def update_isprocessed(asset_id,task_type,is_processed):
    '''
    This function will update the is_processed state in the metadata table

    :param task_type:
    :param group_name:
    :param job_type:
    :param max_value:
    :param source_name:
    :param incremental_column:
    :param metadata_dataset:
    :param metadata_table:
    :param project:
    :return:
    '''
    print("Inside function call")
    # If incremental column has some value, it implies that data is migrated in incremental approach
    # The below code snippet will update the incremental_column_value in metadata table
    if is_processed == "false":
        value = "true"
        # checking if the task is rdbms_to_gcs or rdbms_to_bigquery and selecting ingestion type
        if task_type == 'rdbms_to_gcs' or task_type == "rdbms_to_gcs_dataproc":
            ingestion = 'rdbms_gcs_ingestion_info'
        elif task_type == 'rdbms_to_bq' or task_type == "rdbms_to_bigquery_dataproc":
            ingestion = 'rdbms_bq_ingestion_info'

        cursor = connection.cursor()
        update_incr_value = "UPDATE data_asset_source SET is_processed='{}' WHERE asset_id='{}'".format('Success',asset_id)
        cursor.execute(update_incr_value)
        connection.commit()
        cursor.close()
        return {'result': "Task updated successfully", 'type': "success"}
    
#===========================================================================================

def audit_func_ge_bq(task_name,asset_id,task_id, project,profiling_path,expectations_ui_path,source,source_type,context):
    execution_date = context['execution_date'] - timedelta(0)
    ti = context['ti']
    dag_id = context.get('dag_run').dag_id
    #base_url = "url_composer"
    #from requests import get
    # ip = get('https://api.ipify.org').content.decode('utf8')
    # base_url = ip+':8080'
    dag_execution_date = urllib.parse.quote(str(execution_date))
    dagurl = f"/graph?dag_id={dag_id}&root=&execution_date={dag_execution_date}"
    task_instance = get_task_instance.get_task_instance(dag_id,task_id,execution_date)

    task_state = task_instance.state
    start_time =  "0"
    end_time = "0"
    job_id = "None"
    task_duration = "None"
    if task_state == "success":
        start_time = str(task_instance.start_date)
        # start_time = start_time.split('+')[0]
        end_time = str(task_instance.end_date)
        # end_time = end_time.split('+')[0]
        try:
            if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                print(job_id)
                job_id = job_id['resource']
            else:
                job_id = "None"
        except:
            job_id = "None"
        task_duration = task_instance.duration
    elif task_state == "failed":
        start_time = str(task_instance.start_date)
        # start_time = start_time.split('+')[0]
        end_time = str(task_instance.end_date)
        # end_time = end_time.split('+')[0]
        try:
            if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                print(job_id)
                job_id = job_id['resource']
            else:
                job_id = "None"
        except:
            job_id = "None"
        task_duration = task_instance.duration
    elif task_state == "running":
        start_time = str(task_instance.start_date)
        # start_time = start_time.split('+')[0]
        end_time = str(task_instance.end_date)
        # end_time = end_time.split('+')[0]
        try:
            if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                print(job_id)
                job_id = job_id['resource']
            else:
                job_id = "None"
        except:
            job_id = "None"
        task_duration = task_instance.duration
    print(task_state)

    if task_instance.state == "failed":
        expectations_ui_path = "None as job failed"
        profiling_path = "None as job got failed"
    else:
        expectations_ui_path = expectations_ui_path
        profiling_path = profiling_path

    print(task_state)
    today = str(date.today())
    if str(task_instance.state) == "None" or task_instance.state == None:
        data = {"audit_id": str((context['execution_date']).strftime("%Y%m%d%H%M%S")) + str(dag_id),
                "asset_id":asset_id,
                "job_id": "Loading...",
                "source": str(source),
                "source_type": str(source_type),
                "profiling_path": "Loading...",
                "expectations_ui_path": "Loading...",
                "job_start_time": str(datetime.now()),
                "job_end_time": str(end_time),
                "Task_Duration": str(task_duration),
                "job_status": task_name+"Running",
                "no_of_retries": task_instance.try_number,
                "log_url": str(dagurl),
                "ingestion_date": str(today),
                "dag_id": str(task_instance.dag_id),
                "project": str(project),
                "dataset_id": "dqaudit_dataset_replace",
                "table_id": "dqaudit_table_replace"
                }
    else:
        data = {"audit_id": str((context['execution_date']).strftime("%Y%m%d%H%M%S")) + str(dag_id),
                "asset_id":asset_id,
                "job_id":str(job_id),
                "source":str(source),
                 "source_type":str(source_type),
                "profiling_path":str(profiling_path),
                "expectations_ui_path":str(expectations_ui_path),
                "job_start_time": str(start_time),
                "job_end_time": str(end_time),
                "Task_Duration": str(task_duration),
                "job_status": task_name+str(task_instance.state),
                "no_of_retries": task_instance.try_number,
                "log_url": str(dagurl),
                "ingestion_date": str(today),
                "dag_id": str(task_instance.dag_id),
                "project": str(project),
                "dataset_id": "dqaudit_dataset_replace",
                "table_id": "dqaudit_table_replace"
                }

    print("data is:", data)
    id_token = make_authorized_get_request(endpoint_url_ge)
    headers = {"content-type": "application/json", "Authorization": "Bearer %s" % id_token}
    r = requests.post(endpoint_url_ge, json=data, headers=headers)
    # id_token = make_authorized_get_request(endpoint_url)
    # headers = {"content-type": "application/json", "Authorization": "Bearer %s" % id_token}
    # r = requests.post(endpoint_url, json=data, headers=headers)
    # data = str(data)
    # data = json.dumps(data)
    # # data = json.loads(data)
    # print("json data",data)
    # print(type(data))
    # project_id = project
    # dataset_id = "dqaudit_dataset_replace"
    # table_id = "dqaudit_table_replace"
    #
    # client = bigquery.Client(project=project_id)
    # # dataset = client.dataset(dataset_id)
    # # table = dataset.table(table_id)
    # table = project + '.' + dataset_id + '.' + table_id
    # job_config = bigquery.LoadJobConfig()
    # job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    # job_config.autodetect = True
    # job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    # job = client.load_table_from_json(data, table, job_config=job_config)
    #
    # print(job.result())

#===========================================================================================


# Function for auditing bigquery task
def audit_func_bigquery(job_type,task_type,task_id,project,group_name,item,incremental_column,incremental_column_value,query,metadata_dataset,metadata_table,context):
    execution_date = context['execution_date'] - timedelta(0)
    ti = context['ti']
    dag_id = context.get('dag_run').dag_id
    #base_url = "url_composer"
    # from requests import get
    # ip = get('https://api.ipify.org').content.decode('utf8')
    # base_url = ip+':8080'
    dag_execution_date = urllib.parse.quote(str(execution_date))
    dagurl = f"/graph?dag_id={dag_id}&root=&execution_date={dag_execution_date}"
    task_instance = get_task_instance.get_task_instance(dag_id,task_id,execution_date)

    task_state = task_instance.state
    print(task_state)

    task_start_time=0
    task_end_time=0
    read_count = 0
    write_count = 0
    task_duration = 0
    job_id = "None"
    today = str(date.today())

    source_name = item["source_name"]
    is_processed = item["is_processed"]

    if task_state == "success":

        update_isprocessed(task_type, group_name, job_type, source_name, is_processed, metadata_dataset, metadata_table,project)

        read_count = reading_count(item,query)

        start_time = task_instance.start_date
        end_time = task_instance.end_date

        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)

        a = str(end_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_end_time = c
        print("Task end Time: ", task_end_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
        task_duration = task_instance.duration
        print("calling function write_count")
        write_count = writing_count(item,group_name,job_type, task_start_time, task_end_time, task_type,incremental_column,incremental_column_value)

    elif task_state == "failed":
        start_time = task_instance.start_date
        end_time = datetime.now()

        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)

        a = str(end_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_end_time = c
        print("Task end Time: ", task_end_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"

    elif task_state == "running":

        start_time = task_instance.start_date
        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
    print("job_id is:",job_id)
    state_time = {"success": str(task_start_time), "failed": str(task_start_time), "running": str(task_start_time),"None":str(datetime.now()),None:str(datetime.now())}
    end_time = {"success": str(task_end_time), "failed": str(task_end_time), "running": "None"}
    job_id = {"failed": job_id,"success": job_id, "running": job_id,"None":"Loading...",None:"Loading..."}
    # job_start_time = {"success": str(task_start_time), "failed": "None", "running": str(task_start_time)}
    # job_end_time = {"success": str(task_end_time), "failed": "None", "running": "None"}
    task_duration = {"success": str(task_duration), "failed": "None", "running": "None"}
    source_number_of_records = {"success": read_count, "failed": -1, "running": -1,"None":-1,None:-1}
    destination = {"success": item["dataset_table"], "failed": "None",
                   "running": item["dataset_table"]}
    destination_number_of_records = {"success": write_count, "failed": -1, "running": -1,"None":-1,None:-1}
    status = {"success":"success","failed":"failed","running":"running","None":"Starting...",None:"Starting..."}
    print(status.get(task_state))
    type = job_type + " " + "Rdbms to bigquery"
    data = {"audit_id": str((context['execution_date']).strftime("%Y%m%d%H%M%S")) + dag_id + item['source_name'],
            "group_name": group_name,
            "job_id": job_id.get(task_state),
            "job_type":type,
            "host": item["host"],
            "source_database": item["database"],
            "source_table": item["table_name"],
            "source_number_of_records": source_number_of_records.get(task_state),
            "destination": destination.get(task_state),
            "destination_number_of_records": destination_number_of_records.get(task_state),
            "job_start_time": state_time.get(task_state),
            "job_end_time": end_time.get(task_state),
            "Task_Duration" : task_duration.get(task_state),
            "job_status": status.get(task_state),
            "no_of_retries": task_instance.try_number,
            "log_url": dagurl,
            "ingestion_date": today,
            "dag_id": task_instance.dag_id,
            "audit_dataset":"dataset_to_replace",
            "audit_table":"table_to_replace",
            "project":project
            }

    print("data is:",data)
    id_token = make_authorized_get_request(endpoint_url)
    headers = {"content-type": "application/json", "Authorization": "Bearer %s" % id_token}
    r = requests.post(endpoint_url, json=data, headers=headers)

# #==================================================================================================================
# #==================================================================================================================
# #==================================================================================================================
# #==================================================================================================================

# Function for auditing gcs task
def audit_func_gcs(task_name,asset_id,job_type,outputlocation,task_type,task_id,group_name,item,outputformat,project,incremental_column,incremental_column_value,query,metadata_dataset,metadata_table,context):
    print('item is:',item)
    execution_date = context['execution_date'] - timedelta(0)
    ti = context['ti']
    dag_id = context.get('dag_run').dag_id
    #base_url = "url_composer"
    # from requests import get
    # ip = get('https://api.ipify.org').content.decode('utf8')
    # base_url = ip+':8080'
    dag_execution_date = urllib.parse.quote(str(execution_date))
    print("DAG ID",dag_id)
    dagurl = f"/graph?dag_id={dag_id}&root=&execution_date={dag_execution_date}"
    task_instance = get_task_instance.get_task_instance(dag_id,task_id,execution_date)

    task_state = task_instance.state
    print(task_state)
    print(outputlocation)

    task_start_time=0
    task_end_time=0
    read_count = 0
    task_duration = 0
    write_location = "None"
    write_count = 0
    job_id = "None"
    today = str(date.today())

    source_name = item["source_name"]
    is_processed = item["is_processed"]

    if task_state == "success":
        update_isprocessed(asset_id,task_type,is_processed)
        #=========================================================

        #=========================================================
        read_count = reading_count(item,query)
        # write_location = outputlocation + '/output' + item["source_name"] + str(group_name).lower() + context["ts_nodash"]
        write_location = outputlocation 

        print("write_location",write_location)
        write_count = writing_count_gcs(write_location, task_type,group_name,job_type,item,incremental_column,incremental_column_value,project)
        start_time = task_instance.start_date
        end_time = task_instance.end_date

        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)

        a = str(end_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_end_time = c
        print("Task end Time: ", task_end_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"

        task_duration = task_instance.duration

        write_location = outputlocation

    elif task_state == "failed":
        start_time = task_instance.start_date
        end_time = datetime.now()

        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)

        a = str(end_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_end_time = c
        print("Task end Time: ", task_end_time)

        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
    elif task_state == "running":

        start_time = task_instance.start_date
        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
        write_location = outputlocation + '/output' + item["source_name"] + str(group_name).lower() + context['ts_nodash']
    print("job_id is:", job_id)
    start_time = {"success": str(task_start_time), "failed": str(task_start_time), "running": str(task_start_time),"None":str(datetime.now()),None:str(datetime.now())}
    end_time = {"success": str(task_end_time), "failed": str(task_end_time), "running": "None"}
    job_id = {"failed":job_id,"success": job_id, "running": job_id,"None":"Loading...",None:"Loading..."}
    # job_start_time = {"success": str(task_start_time), "failed": "None", "running": str(task_start_time)}
    # job_end_time = {"success": str(task_end_time), "failed": "None", "running": "None"}
    task_duration = {"success": str(task_duration), "failed": "None", "running": "None"}
    source_number_of_records = {"success": read_count, "failed": -1, "running": -1,"None":-1,None:-1}
    destination = {"success": write_location, "failed": "None",
                   "running": write_location}
    destination_number_of_records = {"success": write_count, "failed": -1,"running": -1,"None":-1,None:-1}
    status = {"success": "success", "failed": "failed", "running": "running","None":"Starting...",None:"Starting..."}
    print(status.get(task_state))
    print("Task_state",task_state)
    type = job_type + " " + "Rdbms to gcs"
    print(str(context['execution_date']))
    data = {"audit_id": str((context['execution_date']).strftime("%Y%m%d%H%M%S")) + dag_id + item['source_name'],
            "asset_id":asset_id,
            "group_name": group_name,
            "job_id": job_id.get(task_state),
            "job_type": type,
            "host": item["host"],
            "source_database": item["database"],
            "source_table": item["table_name"],
            "source_number_of_records": source_number_of_records.get(task_state),
            "destination": destination.get(task_state),
            "destination_number_of_records": destination_number_of_records.get(task_state),
            "job_start_time": start_time.get(task_state),
            "job_end_time": end_time.get(task_state),
            "Task_Duration": task_duration.get(task_state),
            "job_status": task_name+status.get(task_state),
            "no_of_retries": task_instance.try_number,
            "log_url": dagurl,
            "ingestion_date": today,
            "dag_id": task_instance.dag_id,
            "audit_dataset":"dataset_to_replace",
            "audit_table":"table_to_replace",
            "project":project
            }

    print("data is:",data)
    id_token = make_authorized_get_request(endpoint_url)
    headers = {"content-type": "application/json", "Authorization": "Bearer %s" % id_token}
    r = requests.post(endpoint_url, json=data, headers=headers)

# #==================================================================================================================
# #==================================================================================================================
# #==================================================================================================================
# #==================================================================================================================


# Function for auditing gcs to gcs task
def audit_func_gcs_to_gcs(task_name,asset_id,job_type,inputlocation,outputlocation,task_type,task_id,group_name,item,outputformat,project,incremental_column,incremental_column_value,query,metadata_dataset,metadata_table,context):
    print('item is:',item)
    execution_date = context['execution_date'] - timedelta(0)
    ti = context['ti']
    dag_id = context.get('dag_run').dag_id
    #base_url = "url_composer"
    # from requests import get
    # ip = get('https://api.ipify.org').content.decode('utf8')
    # base_url = ip+':8080'
    dag_execution_date = urllib.parse.quote(str(execution_date))
    print("DAG ID",dag_id)
    dagurl = f"/graph?dag_id={dag_id}&root=&execution_date={dag_execution_date}"
    task_instance = get_task_instance.get_task_instance(dag_id,task_id,execution_date)

    task_state = task_instance.state
    print(task_state)
    print(outputlocation)

    task_start_time=0
    task_end_time=0
    read_count = 0
    task_duration = 0
    write_location = "None"
    write_count = 0
    job_id = "None"
    today = str(date.today())

    source_name = item["source_name"]
    is_processed = item["is_processed"]

    if task_state == "success":
        update_isprocessed(asset_id,task_type,is_processed)
        read_location=inputlocation
        #=========================================================

        #=========================================================
        read_count = reading_count_gcs(read_location, task_type,group_name,job_type,item,incremental_column,incremental_column_value,project)
        # write_location = outputlocation + '/output' + item["source_name"] + str(group_name).lower() + context["ts_nodash"]
        write_location = outputlocation 

        print("write_location",write_location)
        write_count = writing_count_gcs(write_location, task_type,group_name,job_type,item,incremental_column,incremental_column_value,project)
        start_time = task_instance.start_date
        end_time = task_instance.end_date

        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)

        a = str(end_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_end_time = c
        print("Task end Time: ", task_end_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"

        task_duration = task_instance.duration

        write_location = outputlocation

    elif task_state == "failed":
        start_time = task_instance.start_date
        end_time = datetime.now()

        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)

        a = str(end_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_end_time = c
        print("Task end Time: ", task_end_time)

        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
    elif task_state == "running":

        start_time = task_instance.start_date
        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
        write_location = outputlocation 
    print("job_id is:", job_id)
    start_time = {"success": str(task_start_time), "failed": str(task_start_time), "running": str(task_start_time),"None":str(datetime.now()),None:str(datetime.now())}
    end_time = {"success": str(task_end_time), "failed": str(task_end_time), "running": "None"}
    job_id = {"failed":job_id,"success": job_id, "running": job_id,"None":"Loading...",None:"Loading..."}
    # job_start_time = {"success": str(task_start_time), "failed": "None", "running": str(task_start_time)}
    # job_end_time = {"success": str(task_end_time), "failed": "None", "running": "None"}
    task_duration = {"success": str(task_duration), "failed": "None", "running": "None"}
    source_number_of_records = {"success": read_count, "failed": -1, "running": -1,"None":-1,None:-1}
    destination = {"success": write_location, "failed": "None",
                   "running": write_location}
    destination_number_of_records = {"success": write_count, "failed": -1,"running": -1,"None":-1,None:-1}
    status = {"success": "success", "failed": "failed", "running": "running","None":"Starting...",None:"Starting..."}
    print(status.get(task_state))
    print("Task_state",task_state)
    type = job_type + " " + "gcs to gcs"
    print(str(context['execution_date']))
    data = {"audit_id": str((context['execution_date']).strftime("%Y%m%d%H%M%S")) + dag_id + item['source_name'],
            "asset_id":asset_id,
            "group_name": group_name,
            "job_id": job_id.get(task_state),
            "job_type": type,
            "host": "GCS bucket",
            "source_database": item["database"],
            "source_table": "",
            "source_number_of_records": source_number_of_records.get(task_state),
            "destination": destination.get(task_state),
            "destination_number_of_records": destination_number_of_records.get(task_state),
            "job_start_time": start_time.get(task_state),
            "job_end_time": end_time.get(task_state),
            "Task_Duration": task_duration.get(task_state),
            "job_status": task_name+status.get(task_state),
            "no_of_retries": task_instance.try_number,
            "log_url": dagurl,
            "ingestion_date": today,
            "dag_id": task_instance.dag_id,
            "audit_dataset":"dataset_to_replace",
            "audit_table":"table_to_replace",
            "project":project
            }

    print("data is:",data)
    id_token = make_authorized_get_request(endpoint_url)
    headers = {"content-type": "application/json", "Authorization": "Bearer %s" % id_token}
    r = requests.post(endpoint_url, json=data, headers=headers)

# #==================================================================================================================
# #==================================================================================================================
# #==================================================================================================================
# #==================================================================================================================

def audit_func_bigquery_pubsub(job_type,output_table, task_type, task_id, group_name, item,project,context):
    execution_date = context['execution_date'] - timedelta(0)
    ti = context['ti']
    dag_id = context.get('dag_run').dag_id
    #base_url = "url_composer"
    # from requests import get
    # ip = get('https://api.ipify.org').content.decode('utf8')
    # base_url = ip+':8080'
    dag_execution_date = urllib.parse.quote(str(execution_date))
    dagurl = f"/graph?dag_id={dag_id}&root=&execution_date={dag_execution_date}"
    task_instance = get_task_instance.get_task_instance(dag_id,task_id,execution_date)

    task_state = task_instance.state
    print(task_state)

    task_start_time=0
    task_end_time=0
    read_count = 0
    write_count = 0
    task_duration = 0
    job_id = "None"
    today = str(date.today())
    if task_state == "success":
        # read_count = reading_count(item)

        start_time = task_instance.start_date
        end_time = task_instance.end_date

        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)

        a = str(end_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_end_time = c
        print("Task end Time: ", task_end_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
        task_duration = task_instance.duration
        # print("calling function write_count")
        # write_count = writing_count(item,group_name,job_type, task_start_time, task_end_time, task_type,incremental_column,incremental_column_value)

    elif task_state == "failed":
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"

    elif task_state == "running":

        start_time = task_instance.start_date
        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
    elif task_state == "None" or task_state == None:

        start_time = datetime.now()
        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"

    print("job_id is:", job_id)
    state_time = {"success": str(task_start_time), "failed": "None", "running": str(task_start_time),
                  "None": str(datetime.now()), None: str(datetime.now())}
    end_time = {"success": str(task_end_time), "failed": "None", "running": "None", "None": "None", None: "None"}
    job_id = {"failed": job_id, "success": job_id, "running": job_id, "None": "Loading...", None: "Loading..."}
    # job_start_time = {"success": str(task_start_time), "failed": "None", "running": str(task_start_time)}
    # job_end_time = {"success": str(task_end_time), "failed": "None", "running": "None"}
    task_duration = {"success": str(task_duration), "failed": "None", "running": "None", "None": "None", None: "None"}
    source_number_of_records = {"success": -1, "failed": -1, "running": -1, "None": -1, None: -1}
    destination = {"success": item["output_table"], "failed": "None",
                   "running": item["output_table"], "None": "None", None: "None"}
    destination_number_of_records = {"success": -1, "failed": -1, "running": -1, "None": -1, None: -1}
    status = {"success": "success", "failed": "failed", "running": "Streaming", "None": "Starting...",
              None: "Starting..."}
    print(status.get(task_state))
    type = job_type + " " + "Pubsub to bigquery"
    data = {"audit_id": str((context['execution_date']).strftime("%Y%m%d%H%M%S")) + dag_id + item['source_name'],
            "group_name": group_name,
            "job_id": job_id.get(task_state),
            "job_type":type,
            "host": "NA",
            "source_database": "NA",
            "source_table": "NA",
            "source_number_of_records": source_number_of_records.get(task_state),
            "destination": destination.get(task_state),
            "destination_number_of_records": destination_number_of_records.get(task_state),
            "job_start_time": state_time.get(task_state),
            "job_end_time": end_time.get(task_state),
            "Task_Duration" : task_duration.get(task_state),
            "job_status": status.get(task_state),
            "no_of_retries": task_instance.try_number,
            "log_url": dagurl,
            "ingestion_date": today,
            "dag_id": task_instance.dag_id,
            "audit_dataset":"dataset_to_replace",
            "audit_table":"table_to_replace",
            "project":project
            }

    print("data is:",data)
    id_token = make_authorized_get_request(endpoint_url)
    headers = {"content-type": "application/json", "Authorization": "Bearer %s" % id_token}
    r = requests.post(endpoint_url, json=data, headers=headers)



# #==================================================================================================================
# #==================================================================================================================
# #==================================================================================================================


def audit_func_gcs_pubsub(job_type,output_gcslocation, task_type, task_id, group_name, item,project,context):
    execution_date = context['execution_date'] - timedelta(0)
    ti = context['ti']
    dag_id = context.get('dag_run').dag_id
    #base_url = "url_composer"
    # from requests import get
    # ip = get('https://api.ipify.org').content.decode('utf8')
    # base_url = ip+':8080'
    dag_execution_date = urllib.parse.quote(str(execution_date))
    dagurl = f"/graph?dag_id={dag_id}&root=&execution_date={dag_execution_date}"
    task_instance = get_task_instance.get_task_instance(dag_id,task_id,execution_date)

    task_state = task_instance.state
    print(task_state)

    task_start_time=0
    task_end_time=0
    read_count = 0
    write_count = 0
    task_duration = 0
    job_id = "None"
    today = str(date.today())
    if task_state == "success":
        # read_count = reading_count(item)

        start_time = task_instance.start_date
        end_time = task_instance.end_date

        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)

        a = str(end_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_end_time = c
        print("Task end Time: ", task_end_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
        task_duration = task_instance.duration
        # print("calling function write_count")
        # write_count = writing_count(item,group_name,job_type, task_start_time, task_end_time, task_type,incremental_column,incremental_column_value)

    elif task_state == "failed":
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"

    elif task_state == "running":

        start_time = task_instance.start_date
        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
    elif task_state == "None" or task_state == None:

        start_time = datetime.now()
        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"

    print("job_id is:",job_id)
    state_time = {"success": str(task_start_time), "failed": "None", "running": str(task_start_time),"None":str(datetime.now()),None:str(datetime.now())}
    end_time = {"success": str(task_end_time), "failed": "None", "running": "None","None":"None",None:"None"}
    job_id = {"failed": job_id,"success": job_id, "running": job_id,"None":"Loading...",None:"Loading..."}
    # job_start_time = {"success": str(task_start_time), "failed": "None", "running": str(task_start_time)}
    # job_end_time = {"success": str(task_end_time), "failed": "None", "running": "None"}
    task_duration = {"success": str(task_duration), "failed": "None", "running": "None","None":"None",None:"None"}
    source_number_of_records = {"success": -1, "failed": -1, "running": -1,"None":-1,None:-1}
    destination = {"success": output_gcslocation, "failed": "None",
                   "running":  output_gcslocation,"None":"None",None:"None"}
    destination_number_of_records = {"success": -1, "failed": -1, "running": -1,"None":-1,None:-1}
    status = {"success":"success","failed":"failed","running":"Streaming","None":"Starting...",None:"Starting..."}
    print(status.get(task_state))
    type = job_type + " " + "Pubsub to gcs"
    data = {"audit_id": str((context['execution_date']).strftime("%Y%m%d%H%M%S")) + dag_id + item['source_name'],
            "group_name": group_name,
            "job_id": job_id.get(task_state),
            "job_type":type,
            "host": "NA",
            "source_database": "NA",
            "source_table": "NA",
            "source_number_of_records": source_number_of_records.get(task_state),
            "destination": destination.get(task_state),
            "destination_number_of_records": destination_number_of_records.get(task_state),
            "job_start_time": state_time.get(task_state),
            "job_end_time": end_time.get(task_state),
            "Task_Duration" : task_duration.get(task_state),
            "job_status": status.get(task_state),
            "no_of_retries": task_instance.try_number,
            "log_url": dagurl,
            "ingestion_date": today,
            "dag_id": task_instance.dag_id,
            "audit_dataset":"dataset_to_replace",
            "audit_table":"table_to_replace",
            "project":project
            }

    print("data is:",data)
    id_token = make_authorized_get_request(endpoint_url)
    headers = {"content-type": "application/json", "Authorization": "Bearer %s" % id_token}
    r = requests.post(endpoint_url, json=data, headers=headers)


# #==================================================================================================================
# #==================================================================================================================
# #==================================================================================================================

def audit_func_sftp(asset_id,job_type,output_path, task_type, task_id, group_name, item,project,context):
    execution_date = context['execution_date'] - timedelta(0)
    ti = context['ti']
    dag_id = context.get('dag_run').dag_id
    #base_url = "url_composer"
    # from requests import get
    # ip = get('https://api.ipify.org').content.decode('utf8')
    # base_url = ip+':8080'
    dag_execution_date = urllib.parse.quote(str(execution_date))
    dagurl = f"/graph?dag_id={dag_id}&root=&execution_date={dag_execution_date}"
    task_instance = get_task_instance.get_task_instance(dag_id,task_id,execution_date)

    task_state = task_instance.state
    print(task_state)

    task_start_time=0
    task_end_time=0
    read_count = 0
    write_count = 0
    task_duration = 0
    job_id = "None"
    today = str(date.today())
    if task_state == "success":
        # read_count = reading_count(item)

        start_time = task_instance.start_date
        end_time = task_instance.end_date

        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)

        a = str(end_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_end_time = c
        print("Task end Time: ", task_end_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
        task_duration = task_instance.duration
        # print("calling function write_count")
        # write_count = writing_count(item,group_name,job_type, task_start_time, task_end_time, task_type,incremental_column,incremental_column_value)

    elif task_state == "failed":
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"

    elif task_state == "running":

        start_time = task_instance.start_date
        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"
    elif task_state == "None" or task_state == None:

        start_time = datetime.now()
        a = str(start_time)
        b = a.split("+")[0]
        c = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
        task_start_time = c
        print("start task Time: ", task_start_time)
        try:
            if job_type == "Dataflow":
                if task_instance.xcom_pull(task_id):
                    job_id = str(task_instance.xcom_pull(task_id).get('dataflow_job_id'))
                else:
                    job_id = "None"
            elif job_type == "Dataproc":
                if task_instance.xcom_pull(key='conf', task_ids=str(task_id)):
                    job_id = task_instance.xcom_pull(key='conf', task_ids=str(task_id))
                    print(job_id)
                    job_id = job_id['resource']
                else:
                    job_id = "None"
        except:
            job_id = "None"

    print("job_id is:",job_id)
    state_time = {"success": str(task_start_time), "failed": "None", "running": str(task_start_time),"None":str(datetime.now()),None:str(datetime.now())}
    end_time = {"success": str(task_end_time), "failed": "None", "running": "None","None":"None",None:"None"}
    job_id = {"failed": job_id,"success": job_id, "running": job_id,"None":"Loading...",None:"Loading..."}
    # job_start_time = {"success": str(task_start_time), "failed": "None", "running": str(task_start_time)}
    # job_end_time = {"success": str(task_end_time), "failed": "None", "running": "None"}
    task_duration = {"success": str(task_duration), "failed": "None", "running": "None","None":"None",None:"None"}
    source_number_of_records = {"success": -1, "failed": -1, "running": -1,"None":-1,None:-1}
    destination = {"success": output_path, "failed": "None",
                   "running":  output_path,"None":"None",None:"None"}
    destination_number_of_records = {"success": -1, "failed": -1, "running": -1,"None":-1,None:-1}
    status = {"success":"success","failed":"failed","running":"running","None":"Starting...",None:"Starting..."}
    print(status.get(task_state))
    type = job_type + " " + task_type
    data = {"audit_id": str((context['execution_date']).strftime("%Y%m%d%H%M%S")) + dag_id + item['source_name'],
            "asset_id": asset_id,
            "group_name": group_name,
            "job_id": job_id.get(task_state),
            "job_type":type,
            "host": item["host"],
            "source_database": "NA",
            "source_table": "NA",
            "source_number_of_records": source_number_of_records.get(task_state),
            "destination": destination.get(task_state),
            "destination_number_of_records": destination_number_of_records.get(task_state),
            "job_start_time": state_time.get(task_state),
            "job_end_time": end_time.get(task_state),
            "Task_Duration" : task_duration.get(task_state),
            "job_status": status.get(task_state),
            "no_of_retries": task_instance.try_number,
            "log_url": dagurl,
            "ingestion_date": today,
            "dag_id": task_instance.dag_id,
            "audit_dataset":"dataset_to_replace",
            "audit_table":"table_to_replace",
            "project":project
            }

    print("data is:",data)
    id_token = make_authorized_get_request(endpoint_url)
    headers = {"content-type": "application/json", "Authorization": "Bearer %s" % id_token}
    r = requests.post(endpoint_url, json=data, headers=headers)

# #==================================================================================================================
# #==================================================================================================================
# #==================================================================================================================



#Read count from the source database
def reading_count(item,query):

    query = query
    database = item["database"]
    drivername = item["driver_name"]
    host = item["host"]
    port = int(item["port"])
    username = item["user_name"]
    secret_alias = item["secret_alias"]
    name = item["secret_alias_path"]
    source_name = item["source_name"]
    secret_client = secretmanager.SecretManagerServiceClient()
    response = secret_client.access_secret_version(name=name)
    secret_mgr = response.payload.data.decode('UTF-8')
    json_secret = json.loads(secret_mgr)
    password = json_secret["password"]
    if "postgre" in drivername:
        try:
            connection = psycopg2.connect(user=username,
                                          password=password,
                                          host=host,
                                          port=port,
                                          database=database)
            cursor = connection.cursor()
            postgreSQL_select_Query = "select count(*) as count from (" + query + ") a"
            cursor.execute(postgreSQL_select_Query)
            result = cursor.fetchall()
            print("read count is :",result[0][0])
            return result[0][0]

        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)
            return

    elif "mysql" in drivername:
        try:
            mydb = mysql.connector.connect(
                host=host,
                user=username,
                password=password,
                database=database
            )

            mycursor = mydb.cursor()
            query = "select count(*) as count from (" + query + ") a"
            mycursor.execute(query)

            result = mycursor.fetchall()
            print("read count is :", result[0][0])
            return result[0][0]
        except :
            print("Error in connection Mysql database")
            return


# Function to get write count from bigquery table (destination)
def writing_count(item,group_name,job_type, task_start_time, task_end_time, task_type,incremental_column,incremental_column_value):
    try:
        if task_type=="rdbms_to_bq" or task_type=="rdbms_to_bigquery_dataproc":
            dataset_table=item["dataset_table"]
            Timestamp = "insertion_timestamp"
            client = bigquery.Client()
            result = client.query("""select count(*) from sandbox-gcp-322412.{} where {} between "{}" and "{}" """.format(dataset_table,Timestamp,str(task_start_time),str(task_end_time)))
            result = result.result().to_dataframe()
            count=int(result['f0_'][0])
            print ("write count is",count)
            return count
    except:
        return 0

def writing_count_gcs(write_location, task_type,group_name,job_type,item,incremental_column,incremental_column_value,project):
    try:
        print(task_type)
        if task_type=="rdbms_to_gcs" or task_type=="gcs_to_gcs" or task_type=="gcs_to_gcs_dataproc" or task_type == "rdbms_to_gcs_dataproc":
            count = 0
            client = bigquery.Client()
            storage_client = storage.Client()
            bucket_name = write_location.split('//')[1].split('/')[0]
            bucket = storage_client.bucket(bucket_name)
            print("bucket is",bucket)
            temp = bucket_name + '/'
            directory = write_location.split(temp)[1]
            directory = directory + '/'
            print("directory is", directory)
            blobs = storage_client.list_blobs(bucket_name, prefix=directory)
            # external_config = bigquery.ExternalConfig("CSV")
            print("blob is:", blobs)
            files_list = ['gs://' + bucket_name + '/' + blob.name for blob in blobs]
            # external_config.source_uris = files_list
            for file in files_list:
                if str(file).endswith('.csv'):
                    uri = file
                    print("file:",uri)
                    temp_count = subprocess.getoutput(f'gsutil cat {uri} | grep -v ^$| wc - l')
                    temp_count=int(temp_count.split()[0])
                    # print(temp_count)
                    # print(type(temp_count))
                    temp_count =temp_count-1
                    print("temp_count is",temp_count)
                    count = count+temp_count
                elif str(file).endswith('.parquet'):
                    fs = gcsfs.GCSFileSystem(project=project)
                    dataset = ParquetDataset(str(file),filesystem=fs)
                    for piece in dataset.pieces:
                        count = count+piece.get_metadata().num_rows

            print("write count is:",count)
            return count
    except:
        return 0
    
def reading_count_gcs(write_location, task_type,group_name,job_type,item,incremental_column,incremental_column_value,project):
    try:
        print(task_type)
        if task_type=="rdbms_to_gcs" or task_type=="gcs_to_gcs" or task_type=="gcs_to_gcs_dataproc" or task_type == "rdbms_to_gcs_dataproc":
            count = 0
            client = bigquery.Client()
            storage_client = storage.Client()
            bucket_name = write_location.split('//')[1].split('/')[0]
            bucket = storage_client.bucket(bucket_name)
            print("bucket is",bucket)
            temp = bucket_name + '/'
            directory = write_location.split(temp)[1]
            directory = directory + '/'
            print("directory is", directory)
            blobs = storage_client.list_blobs(bucket_name, prefix=directory)
            # external_config = bigquery.ExternalConfig("CSV")
            print("blob is:", blobs)
            files_list = ['gs://' + bucket_name + '/' + blob.name for blob in blobs]
            # external_config.source_uris = files_list
            for file in files_list:
                if str(file).endswith('.csv'):
                    uri = file
                    print("file:",uri)
                    temp_count = subprocess.getoutput(f'gsutil cat {uri} | grep -v ^$| wc - l')
                    temp_count=int(temp_count.split()[0])
                    # print(temp_count)
                    # print(type(temp_count))
                    temp_count =temp_count-1
                    print("temp_count is",temp_count)
                    count = count+temp_count
                elif str(file).endswith('.parquet'):
                    fs = gcsfs.GCSFileSystem(project=project)
                    dataset = ParquetDataset(str(file),filesystem=fs)
                    for piece in dataset.pieces:
                        count = count+piece.get_metadata().num_rows

            print("write count is:",count)
            return count
    except:
        return 0