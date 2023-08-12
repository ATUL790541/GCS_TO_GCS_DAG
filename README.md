# GCS_TO_GCS_DAG


This is GCS to GCS dag where a file is stored in GCS bucket and then copied to landing zone through ingestion job in dag and then from landing zone we are performing DLP tokenization on the sensitive column using surrogate key and predefined deidentify templates then file is moved to secured landing zone where we are performing DATA QUALITY task and generating several profiling reports.All these takes place on DATAPROC Clusters which we are creating and deleting once the above task are done.The dag also contain audit task where we are writing information back to postgre table using cloud function implemented in GCS Cloud.
