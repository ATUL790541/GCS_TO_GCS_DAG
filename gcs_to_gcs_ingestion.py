from pyspark.sql import SparkSession
from pyspark.sql import functions
import os
from google.cloud import bigquery
import argparse
import logging
import random

def main():
    try:
        db_args, pipeline_args = get_args()
        # Reading arguments
        file_path = db_args.file_path
        project = db_args.project
        db_table = db_args.region
        output_file_path = db_args.output_location
        dw_load_ind = db_args.dw_load_ind
        bigquery_table=db_args.bigquery_table
        temporary_gcs_bucket=db_args.temporary_gcs_bucket
        jar_path = db_args.jar_path
        #spark = SparkSession.builder.appName("Read CSV in PySpark").getOrCreate()
        #spark = SparkSession.builder.master("yarn").appName("CSV_TO_BIG_QUERY").config("spark.jars", jar_path).config('spark.driver.extraClassPath', jar_path).getOrCreate()
        spark = SparkSession.builder \
        .appName('1.2. BigQuery Storage & Spark SQL - Python')\
        .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar') \
        .getOrCreate()           
        print(temporary_gcs_bucket)
        print(bigquery_table)
        print("DW value",dw_load_ind)
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        df.show()
        df.write.csv(output_file_path, header=True, mode='overwrite')
        print("Writing Success")
        #df.write.format("bigquery").option("temporaryGcsBucket","temp_bucket_23").option("project", "gcp-accelerator-380712").option("dataset", "housing_dat").option("table", "housing_table").mode("append").save()
        if dw_load_ind == 'True':
            df.write.format("bigquery").option("temporaryGcsBucket",temporary_gcs_bucket).option("project",project).option("dataset", "housing_dat").option("table", bigquery_table).mode("append").save()
        print("DW SUCCESS")
    except Exception as e:
        logging.error("Exception thrown in main: ", exc_info=e)
        raise

    


def get_args():
    try:
        logging.info("In get_args() to fetch arguments")
        parser = argparse.ArgumentParser()
        parser.add_argument('--project', dest='project', default=None)
        parser.add_argument('--region', dest='region', default=None)
        parser.add_argument('--jar_path', dest='jar_path', default=None)
        parser.add_argument('--file_path', dest='file_path', default=None)
        parser.add_argument('--outputlocation', dest='output_location', default=None)
        parser.add_argument('--dw_load_ind', dest='dw_load_ind', default=None)
        parser.add_argument('--bigquery_table', dest='bigquery_table', default=None)
        parser.add_argument('--temporary_gcs_bucket', dest='temporary_gcs_bucket', default=None)
        parsed_db_args, pipeline_args = parser.parse_known_args()
        logging.info("Read arguments from composer")
        return parsed_db_args, pipeline_args
    except Exception as e:
        logging.error("Exception in get_args(): ", exc_info=e)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("In main: Job Started gcs to gcs via dataproc")
    main()
