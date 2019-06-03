"""
### Copy cloudsql tables to bigquery

superstision
"""
import os
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

default_args = {
    'owner': 'anders',
    'depends_on_past': False,
    'start_date': datetime.now(),  # Change this to a literal date to make scheduler work.
    'email': ['anders@...'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('copy_cloudsql_to_bigquery_v2',
          default_args=default_args,
          schedule_interval='0 6 * * *')
dag.doc_md = __doc__


class TableConfig:
    STANDARD_EXPORT_QUERY = None
    _STANDARD_EXPORT_QUERY = "SELECT * from {}"

    def __init__(self,
                 cloud_sql_instance,
                 export_bucket,
                 export_database,
                 export_table,
                 export_query,
                 gcp_project,
                 stage_dataset,
                 stage_table,
                 stage_final_query,
                 bq_location
                 ):

        self.params = {
            'export_table': export_table,
            'export_bucket': export_bucket,
            'export_database': export_database,
            'export_query': export_query or self._STANDARD_EXPORT_QUERY.format(export_table),
            'gcp_project': gcp_project,
            'stage_dataset': stage_dataset,
            'stage_table': stage_table or export_table,
            'stage_final_query': stage_final_query,
            'cloud_sql_instance': cloud_sql_instance,
            'bq_location': bq_location or "EU",
        }


def get_tables():
    """
    return a list of tables that should go from cloud sql to bigquery
    In this example all 3 tables reside in the same cloud sql instance.
    :return:
    """
    dim_tables = ["DimAge", "DimPerson"]
    fact_tables = ["FactPerson"]
    export_tables = dim_tables + fact_tables
    tables = []
    for dim in export_tables:
        tables.append(TableConfig(cloud_sql_instance='CLOUD_SQL_INSTANCE_NAME',
                                  export_table=dim,
                                  export_bucket='YOUR_STAGING_BUCKET',
                                  export_database='prod',
                                  export_query=TableConfig.STANDARD_EXPORT_QUERY,
                                  gcp_project="YOUR_PROJECT_ID",
                                  stage_dataset="YOUR_STAGING_DATASET",
                                  stage_table=None,
                                  stage_final_query=None,
                                  bq_location="EU"))
    return tables

def gen_export_table_task(table_config):
    export_task = MySqlToGoogleCloudStorageOperator(task_id='export_{}'.format(table_config.params['export_table']),
                                                    dag=dag,
                                                    sql=table_config.params['export_query'],
                                                    bucket=table_config.params['export_bucket'],
                                                    filename="cloudsql_to_bigquery/{}/{}".format(table_config.params['export_table'],
                                                                                                 table_config.params['export_table']) + "_{}",
                                                    schema_filename="cloudsql_to_bigquery/schema/{}/schema_raw".format(table_config.params['export_table']),
                                                    mysql_conn_id="gcp_dvh_cloudsql")
    export_task.doc_md = """\
    #### Export table from cloudsql to cloud storage
    task documentation
    """
    return export_task


def gen_import_table_task(table_config):
    import_task = GoogleCloudStorageToBigQueryOperator(
        task_id='{}_to_bigquery'.format(table_config.params['export_table']),
        bucket=table_config.params['export_bucket'],
        source_objects=["cloudsql_to_bigquery/{}/{}*".format(table_config.params['export_table'],
                                                             table_config.params['export_table'])],
        destination_project_dataset_table="{}.{}.{}".format(table_config.params['gcp_project'],
                                                            table_config.params['stage_dataset'],
                                                            table_config.params['stage_table']),
        schema_object="cloudsql_to_bigquery/schema/{}/schema_raw".format(table_config.params['export_table']),
        write_disposition='WRITE_TRUNCATE',
        source_format="NEWLINE_DELIMITED_JSON",
        dag=dag)

    import_task.doc_md = """\
        #### Import table from storage to bigquery
        task documentation    
        """
    return import_task


"""
The code that follows setups the dependencies between the tasks
"""

for table_config in get_tables():
    export_script = gen_export_table_task(table_config)
    import_script = gen_import_table_task(table_config)

    export_script >> import_script
