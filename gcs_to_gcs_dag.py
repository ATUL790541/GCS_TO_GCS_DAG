from airflow.models import DAG, Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.dates import days_ago
import json
from google.cloud import storage
from google.cloud import bigquery

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
#from utils.helper_tdf import audit_func_gcs, reading_count
from utils.helper import audit_func_gcs,reading_count, audit_func_ge_bq
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreateWorkflowTemplateOperator,
    DataprocDeleteClusterOperator,
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
    ClusterGenerator,
)
from airflow.utils.trigger_rule import TriggerRule
import time
import os
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from functools import partial
import tdf_uni_config_backend
import ast
from google.cloud import secretmanager
from airflow.models import Variable
import psycopg2
import pandas as pd
import logging
from airflow.operators.python import BranchPythonOperator

default_args = {
    'start_date': days_ago(1),
    'depends_on_past': False
}



source_system_id = '103'
asset_id = '1002'
cluster_code = 'L1'
job_frequency = 'once'
#job_frequency_type = job_frequency_type_to_replace
cron_tab = ''
job_type = 'dataproc'

source_name = asset_id
cluster_name = 'tdf-unified-' + asset_id
project = tdf_uni_config_backend.PROJECT
region = tdf_uni_config_backend.REGION
pipeline_src_code_loc = tdf_uni_config_backend.SOURCECODE_GCS_DATAPROC
tokenization_dlp_dp= tdf_uni_config_backend.TOKENIZATION_DLP_DATAPROC
cursor = tdf_uni_config_backend.connection.cursor()
cluster_query = "select * from cluster_conf where cluster_code = '{}';".format(cluster_code)
#print(cluster_query)
cursor.execute(cluster_query)
cluster_info = cursor.fetchall()
cursor.close()

column_names = [desc[0] for desc in cursor.description]
cluster_json_conf = pd.DataFrame(cluster_info, columns=column_names)

cluster_json_conf = cluster_json_conf.to_dict('records')
for cluster_item in cluster_json_conf:
    cluster_project_id = cluster_item["project_id"]
    cluster_zone = cluster_item["zone"]
    cluster_master_disk_size = cluster_item['master_disk_size']
    cluster_master_machine_type = cluster_item['master_machine_type']
    cluster_worker_machine_type = cluster_item['worker_machine_type']
    cluster_num_workers = cluster_item['num_workers']
    cluster_image_version = cluster_item['image_version']
    #cluster_metadata = ast.literal_eval(cluster_item['metadata'])
    cluster_metadata = {"spark-bigquery-connector-version": "0.17.1"}
    cluster_region = cluster_item['region']
    cluster_init_actions_uris = ast.literal_eval(tdf_uni_config_backend.init_action_uris)


# create a cluster config
CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=cluster_project_id,
    zone=cluster_zone,
    master_disk_size=cluster_master_disk_size,
    master_machine_type=cluster_master_machine_type,
    worker_machine_type=cluster_worker_machine_type,
    num_workers=cluster_num_workers,
    metadata=cluster_metadata,
    service_account="483943278625-compute@developer.gserviceaccount.com",
    service_account_scopes=["https://www.googleapis.com/auth/cloud-platform"],
    init_actions_uris=cluster_init_actions_uris

).make()


#GCS_FILE_PATH = "gs://gcs_to_gcs_bucket/Education_Loan.csv"




def load_config_from_db(**kwargs):
    #db_jar_path = data['db_jar_path']
    deidentify_temp_path="projects/gcp-accelerator-380712/locations/us-central1/deidentifyTemplates/dlptestdeindentify"
    data_quality_check = 'True'
    tokenization_check = 'True'
    landing_zone = "gs://"+tdf_uni_config_backend.LANDING_ZONE+"/dataproc/"+str(asset_id)+ "_"+"{{ts_nodash}}"
    outputformat = "csv"
    task_type = "gcs_to_gcs"
    jar_path = tdf_uni_config_backend.GCS_COMPONENTS_JARS
    gcs_file_path = "gs://gcs_to_gcs_bucket/Complex_Datatypes.csv"
    group_name = "grp1DE"
    dw_load_ind = 'True'
    metadata_table = ""

    incremental_column = ""
    incremental_column_value = ""
    column_partition = ""
    query = ""
    
    Variable.set('incremental_column', incremental_column)
    Variable.set('incremental_column_value', incremental_column_value)

    jar_path = tdf_uni_config_backend.GCS_COMPONENTS_JARS
    Variable.set('output_file', landing_zone)
    Variable.set('outputformat', outputformat)
    Variable.set('task_type', task_type)
    Variable.set('jar_path', jar_path)
    Variable.set('group_name', group_name)
    Variable.set('query', query)
    Variable.set('metadata_dataset', metadata_dataset)
    Variable.set('metadata_table', metadata_table)
    Variable.set('column_partition', column_partition)
    Variable.set('tokenization_check', tokenization_check)
    Variable.set('data_quality_check', data_quality_check)
    Variable.set('deidentify_path',deidentify_temp_path)
    Variable.set('file_read_path',gcs_file_path)       
    Variable.set('output_file', landing_zone)
    Variable.set('tokenization_check', tokenization_check)
    Variable.set('data_quality_check', data_quality_check)    
    Variable.set('dw_load_ind', dw_load_ind)
    
def determine_branch(**kwargs):
    isTokenization = Variable.get('tokenization_check')
    print(tokenization_dlp_dp)
    isDqCheck = Variable.get('data_quality_check')
    if isDqCheck == 'True':
        #json_data = {"Great Expectations":{"column_values_to_be_not_null":{"columns":["customer_id","customer_name","segment"]},"column_value_int_data_type_check":{"columns":["age"]},"column_value_string_data_type_check":{"columns":["region"]},"column_value_long_data_type_check":{"columns":["postal_code"]},"column_to_exist":{"columns":["customer_id","customer_name","segment","age","country","city","state","postal_code","region","timestamp","insertion_timestamp"]},"column_values_to_be_unique":{"columns":["customer_id","customer_name"]},"column_values_to_contain_valid_email":{"columns":["customer_id","customer_name"]},"match_strftime_format":{"columns":["timestamp","insertion_timestamp"]},"column_values_not_contain_special_character":{"columns":["customer_id","customer_name"]},"column_max_to_be_between":{"columns":["age",[10,70],"postal_code",[1,7]]},"column_value_lengths_to_be_between":{"columns":["customer_id",[1,10],"state",[1,5]]},"column_value_lengths_to_equal":{"columns":["city",[9]]},"column_values_to_be_dateutil_parseable":{"columns":["timestamp"]},"column_values_to_be_in_type_list":{"columns":["customer_id",["String"]]},"column_values_to_be_null":{"columns":["customer_id","customer_name","segment"]},"table_column_count_to_be_between":{"values":[1,5]},"table_column_count_to_equal":{"values":[15]},"table_row_count_to_be_between":{"values":[1,10000]},"table_row_count_to_equal":{"values":[10000]},"column_mean_to_be_between":{"columns":["age",[1,10],"postal_code",[1,5]]},"column_median_to_be_between":{"columns":["age",[1,10],"postal_code",[1,5]]},"column_min_to_be_between":{"columns":["age",[1,10],"postal_code",[1,5]]},"column_sum_to_be_between":{"columns":["age",[1,10],"postal_code",[1,5]]},"column_proportion_of_unique_values_to_be_between":{"columns":["customer_id",[1,10],"postal_code",[1,5]]},"column_unique_value_count_to_be_between":{"columns":["customer_id",[1,1000],"postal_code",[1,50]]}}}
        #json_data = {"Great_Expectations":["expect_column_to_exist('int')", "expect_column_proportion_of_unique_values_to_be_between('int',1,10)"]}
        #json_data = ["expect_column_values_to_be_null('int')", "expect_column_to_exist('int')", "expect_column_proportion_of_unique_values_to_be_between('int',1,10)"]
        json_data = ["expect_column_values_to_be_null('masterCard')", "expect_column_to_exist('masterCard')", "expect_column_proportion_of_unique_values_to_be_between('masterCard',1,10)"]
        json_data = json.dumps(json_data)
        Variable.set('data_quality_expectations', json_data)
    if isTokenization == 'True' and isDqCheck == 'True':
        return ['TOKENIZATION_JOB' + source_name, "DQ" + source_name, 'DELETE_DATAPROC_CLUSTER']
    elif isTokenization == 'False' and isDqCheck == 'True':
        return ["DQ" + source_name, 'DELETE_DATAPROC_CLUSTER']
    elif isTokenization == 'True' and isDqCheck == 'False':
        return ['TOKENIZATION_JOB' + source_name, 'DELETE_DATAPROC_CLUSTER']
    else:
        return ['DELETE_DATAPROC_CLUSTER']  


def audit(task_name,asset_id,job_type,inputlocation, outputlocation, task_type, task_id, group_name, item,outputformat,project,incremental_column,incremental_column_value,query,metadata_dataset,metadata_table,**context):
    audit_func_gcs_to_gcs(task_name,asset_id,job_type,inputlocation,outputlocation, task_type, task_id, group_name, item,outputformat,project,incremental_column,incremental_column_value,query,metadata_dataset,metadata_table,context)


def audit_running(task_name,asset_id,job_type,inputlocation,outputlocation, task_type, task_id, group_name, item,outputformat,project,incremental_column,incremental_column_value,query,metadata_dataset,metadata_table,context):
    audit_func_gcs_to_gcs(task_name,asset_id,job_type,inputlocation,outputlocation, task_type, task_id, group_name, item,outputformat,project,incremental_column,incremental_column_value,query,metadata_dataset,metadata_table,context)




with DAG('GCS_to_GCS_dag', schedule_interval=None, default_args=default_args, catchup=False, tags=['GCS_GCS_DATAPROC_L1']) as dag:
    LOAD_CONFIG = PythonOperator(task_id='LOAD_CONFIG_FROM_DB', python_callable=load_config_from_db)

    CREATE_DATAPROC_CLUSTER = DataprocCreateClusterOperator(
        task_id='CREATE_DATAPROC_CLUSTER',
        cluster_name=cluster_name,
        project_id=project,
        region=region,
        cluster_config=CLUSTER_GENERATOR_CONFIG
    )
    
    jar_path = Variable.get('jar_path')
    group_name = Variable.get('group_name')
    deidentify_path = Variable.get('deidentify_path', default_var=0)
    isTokenization = Variable.get('tokenization_check')
    read_file_path = Variable.get('file_read_path')
    output_file = Variable.get('output_file')
    task_type=Variable.get('task_type')
    outputformat=Variable.get('outputformat')
    incremental_column=Variable.get('incremental_column')
    incremental_column_value=Variable.get('incremental_column_value')
    metadata_dataset=Variable.get('metadata_dataset')
    metadata_table=Variable.get('metadata_table')
    query=Variable.get('query')
    dw_load_ind = Variable.get('dw_load_ind')
    #data_quality_expectations = Variable.get('data_quality_expectations', default_var=0)
    isTokenization = Variable.get('tokenization_check')
    data_quality_expectations = Variable.get('data_quality_expectations', default_var=0)
    file_name = "ge_result_gcs_ui_" + "{{ts_nodash}}" + ".html"
    profiling_file_name = "ge_result_gcs_profiling_ui_" + "{{ts_nodash}}" + ".html"
    read_file_path_audit=read_file_path.rsplit('/', 1)[0]
    


    
    item = {"source_name": source_name,"database":"gs://" + tdf_uni_config_backend.LANDING_ZONE + "/dataproc/"+ source_name + str(group_name).lower() + "{{ts_nodash}}"+"/","is_processed": "true"}

    output_file_gcs = "gs://" + tdf_uni_config_backend.LANDING_ZONE + "/dataproc"
    task_id = "INGESTION_JOB" + source_name
    task_name='Ingestion_'
    task_name_tokenize='tokenization_'
    bigquery_table = 'housing_table'
    temp_gcs_loc = 'temp_bucket_23'
    #item = {"source_name": source_name,"host":host, "database":database, "table_name":table,"driver_name":drivername,"port":port ,"user_name":username ,"secret_alias": source_system_id,"secret_alias_path":"projects/gcp-accelerator-380712/secrets/" + source_system_id + "/versions/latest","is_processed": "true"}
    SUBMIT_INGESTION_JOB = DataProcPySparkOperator(
        task_id="INGESTION_JOB" + source_name,
        region=region,
        project_id=project,
        cluster_name=cluster_name,
        main='gs://storage_demo_1/gcs_to_gcs_ingestion.py',
        arguments=['--project', project,
                   '--region', region,
                   '--jar_path', jar_path,
                   '--dw_load_ind', dw_load_ind,
                   '--outputlocation', output_file,
                   '--file_path', read_file_path,
                   '--bigquery_table', bigquery_table,
                   '--temporary_gcs_bucket', temp_gcs_loc
                   ],
        dataproc_jars=[jar_path],
        retries=0,
        #on_execute_callback=partial(audit_running, task_name, asset_id, job_type, output_file_gcs, task_type, task_id,
                                    #group_name, item, outputformat, project, incremental_column,
                                    #incremental_column_value, query, metadata_dataset, metadata_table),
        
        on_execute_callback=partial(audit_running, task_name, asset_id, job_type, read_file_path_audit, output_file, task_type,
                                    task_id, group_name, item, outputformat, project, incremental_column,
                                    incremental_column_value, query, metadata_dataset, metadata_table),

        dag=dag
    )
    dlp_output_path = 'gs://'+tdf_uni_config_backend.SEC_LANDING_ZONE+'/dlp_output/'
    pyspark_tokenization = tokenization_dlp_dp
    TOKENIZATION_JOB = DataProcPySparkOperator(
        region=region,
        main=pyspark_tokenization,
        task_id='TOKENIZATION_JOB'+source_name,
        arguments=["--input_file_path", output_file,
                   "--deidentify_path",deidentify_path,
                   "--dlp_output_path", dlp_output_path
                   ],
        cluster_name=cluster_name,
        #dataproc_jars=["gs://tdf-tdlcomponents/components/jars/jars_postgresql-42.2.18.jar"],
        retries=0,
        #on_execute_callback=partial(audit_running, task_name_tokenize, asset_id, job_type, output_file_gcs, task_type,
                                    #task_id, group_name, item, outputformat, project, incremental_column,
                                    #incremental_column_value, query, metadata_dataset, metadata_table),
        
        on_execute_callback=partial(audit_running, task_name_tokenize, asset_id, job_type, read_file_path_audit, output_file, task_type,
                                    task_id, group_name, item, outputformat, project, incremental_column,
                                    incremental_column_value, query, metadata_dataset, metadata_table),
        dag=dag
    )
    if isTokenization == 'True':
        split_string = output_file.split('gs://tdf_landing_zone')[1]
        dq_source_file = 'gs://' + tdf_uni_config_backend.SEC_LANDING_ZONE + '/dlp_output' + split_string +'/*.csv'
        #dq_source_file = "gs://tdf-output/dataflow/outputdftest1805grp2de20230518T094014/file-00000-of-00001.csv"
    else:
        dq_source_file = output_file+ '/*.csv'
        #dq_source_file = "gs://tdf-output/dataflow/outputdftest1805grp2de20230518T094014/file-00000-of-00001.csv"
    gcs_output_location = tdf_uni_config_backend.SEC_LANDING_ZONE
    file_name = "ge_result_gcs_ui_" + "{{ts_nodash}}" + ".html"
    profiling_file_name = "ge_result_gcs_profiling_ui_" + "{{ts_nodash}}" + ".html"

    profiling_path = "https://storage.cloud.google.com/" + tdf_uni_config_backend.SEC_LANDING_ZONE + "/" + profiling_file_name
    expectations_ui_path = "https://storage.cloud.google.com/" + tdf_uni_config_backend.SEC_LANDING_ZONE + "/" + file_name

    dq_spark = tdf_uni_config_backend.DATAQUALITY_DATAPROC
    task_name_dq = 'Data_Quality_'
    DATAQUALITY_TASK = DataProcPySparkOperator(
        task_id="DQ" + source_name,
        region=region,
        project_id=project,
        cluster_name=cluster_name,
        main=dq_spark,
        arguments=[
            "--source_file", dq_source_file,
            "--gcs_output_location", gcs_output_location,
            "--file_name", file_name,
            "--profiling_file_name", profiling_file_name,
            "--data_quality_expectations", str(["expect_column_values_to_be_null('int')", "expect_column_to_exist('int')", "expect_column_proportion_of_unique_values_to_be_between('int',1,10)"])
         ],
         retries=0,
        # on_execute_callback=partial(audit_dq, task_name_dq, asset_id, profiling_path, expectations_ui_path, task_id, project,
                                    # dq_source_file, "GCS"),
        dag=dag
    )
    AUDIT_TASK = PythonOperator(task_id="audit" + task_id+ str(group_name).lower(),
                            python_callable=audit,
                            provide_context=True,
                            op_kwargs={"task_name":task_name_tokenize,"asset_id":asset_id,"job_type": "Dataflow","inputlocation":read_file_path_audit, "outputlocation":output_file, "task_type": task_type, "task_id": task_id,
                                        "group_name":group_name,"item": item,"outputformat":outputformat,"project":project,"incremental_column":incremental_column,"incremental_column_value":incremental_column_value,"query":query,"metadata_dataset":metadata_dataset,"metadata_table":metadata_table},
                            trigger_rule=TriggerRule.ALL_DONE,
                            dag=dag)

    AUDIT_TASK_START = PythonOperator(task_id="audit_start_" + task_id + str(group_name).lower(),
                                python_callable=audit,
                                provide_context=True,
                                op_kwargs={"task_name":task_name,"asset_id":asset_id,"job_type": "Dataflow","inputlocation":read_file_path_audit, "outputlocation": output_file,
                                            "task_type": task_type, "task_id": task_id,
                                            "group_name": group_name, "item": item, "outputformat": outputformat,
                                            "project": project, "incremental_column": incremental_column,
                                            "incremental_column_value": incremental_column_value, "query": query,
                                            "metadata_dataset": metadata_dataset, "metadata_table": metadata_table},
                                trigger_rule=TriggerRule.ALL_DONE,
                                dag=dag)
    
    DELETE_DATAPROC_CLUSTER = DataprocDeleteClusterOperator(
        task_id='DELETE_DATAPROC_CLUSTER',
        cluster_name=cluster_name,
        project_id=cluster_project_id,
        region=region,
        dag=dag
    )

#LOAD_CONFIG >> CREATE_DATAPROC_CLUSTER>>SUBMIT_INGESTION_JOB>>TOKENIZATION_JOB>>DATAQUALITY_TASK
AUDIT_TASK_START >> LOAD_CONFIG >> CREATE_DATAPROC_CLUSTER>>SUBMIT_INGESTION_JOB>>TOKENIZATION_JOB>>DATAQUALITY_TASK>>AUDIT_TASK>>DELETE_DATAPROC_CLUSTER