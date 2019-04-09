"""
### Copy cloudsql tables to bigquery

Flow is this

1. Export table from cloud sql to storage bucket
    -> do some fixes on the export.
2. Export schema to storage bucket
    -> Create bigwuery schema from it
2. Import from storage bucket to bigquery stage location
3. Run a query in bigquery that will join the stage table with the existing table and overwrite.
"""
import os
import logging
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),  # CHANGEME!
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('copy_cloudsql_to_bigquery', default_args=default_args)
dag.doc_md = __doc__


# Just for visual completness
start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)


class TableConfig:
    """
    Holds config for the export/import job we are creating.
    """
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
    """
    Create a export table task for the current table.  preserves table_config parameters for later use.
    :return: a task to be used.
    """
    export_script = BashOperator(
                                task_id='export_{}_with_gcloud'.format(table_config.params['export_table']),
                                params=table_config.params,
                                bash_command="""
gcloud --project {{ params.gcp_project }} sql export csv {{ params.cloud_sql_instance }} \
                            gs://{{ params.export_bucket }}/{{ params.export_table }}_{{ ds_nodash }} \
                            --database={{ params.export_database }} --query="{{ params.export_query }}"
""",
                                dag=dag)
    export_script.doc_md = """\
    #### Export table from cloudsql to cloud storage
    task documentation
    """

    return export_script


def gen_export_schema_task(table_config):
    """
    Generate an export for the myssql schema
    """
    export_script = BashOperator(
                                task_id='export_schema_{}_with_gcloud'.format(table_config.params['export_table']),
                                params=table_config.params,
                                bash_command="""
gcloud --project {{ params.gcp_project }} sql export csv {{ params.cloud_sql_instance }} \
                            gs://{{ params.export_bucket }}/{{ params.export_table }}_{{ ds_nodash }}_schema_raw \
                            --database={{ params.export_database }} \
                            --query="SELECT COLUMN_NAME,DATA_TYPE  FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_SCHEMA = '{{ params.export_database }}' AND TABLE_NAME = '{{ params.export_table }}' order by ORDINAL_POSITION;"
""",
                                dag=dag)
    export_script.doc_md = """\
    #### Export table from cloudsql to cloud storage
    task documentation
    """

    return export_script


def datatype_to_bq(datatype):
    """
    Simple attempt to do some basic datatypes.
    Fallback to String for unknowns and then you can fix it later in bigquery.
    """
    if "DATETIME" in datatype:
        return "DATETIME"
    if "DATE" in datatype:
        return "DATE"
    if "INT" in datatype:
        return "INTEGER"
    if "FLOAT" in datatype or "DOUBLE" in datatype or "DECIMAL" in datatype:
        return "FLOAT"
    return "STRING"


def create_bigquery_schema_from_kv(**kwargs):
    """
    Translates csv schema file to a bigquery json file.
    ID:INT
    foo:Text

    Will become

    [ {"name": "id", "type": "INTEGER", "mode": "nullable"},
      {"name": "foo", "type": "STRING", "mode": "nullable"}
    ]
    """
    bucket = "gs://{}/".format(kwargs['export_bucket'])
    raw_file = "{}_{}_schema_raw".format(kwargs['export_table'], kwargs['ds_nodash'])
    os.system("gsutil cp {}{} {}".format(bucket, raw_file, raw_file))
    logging.info("Grabbed raw schmea file to {}".format(raw_file))
    bigquery_schema = []
    with open(raw_file) as f:
        for line in f.readlines():
            name,datatype = line.replace(" ", "_").replace("/", "_and_").replace('"', "").split(",")
            bq_datatype = datatype_to_bq(datatype.upper())
            bigquery_schema.append(' {{\n   "name": "{}",\n   "type": "{}",\n   "mode": "NULLABLE"\n  }}\n'.format(name, bq_datatype))

    schema = "[\n  {}]".format(",".join(bigquery_schema))
    filename = "{}_json".format(raw_file[:-4])
    with open(filename, "w") as text_file:
        text_file.write(schema)

    os.system("gsutil cp {} {}{}".format(filename, bucket,filename))


def gen_create_bigquery_schema_from_export(table_config):
    task = PythonOperator(
        task_id='create_bigquery_schema_for_{}'.format(table_config.params['export_table']),
        python_callable=create_bigquery_schema_from_kv,
        provide_context=True,
        op_kwargs=table_config.params,
        dag=dag)
    return task


def gen_import_table_task(table_config):
    """
    First copies json schema file from stage bucket (created by previous tasks)
    cat content for debugging
    the try to load into bigquery.
    :param table_config:
    :return:
    """
    import_script = BashOperator(
        task_id='import_{}_to_bigquery'.format(table_config.params['export_table']),
        params=table_config.params,
        bash_command="""
gsutil cp gs://{{ params.export_bucket }}/{{ params.export_table }}_{{ ds_nodash }}_schema_json {{ params.export_table }}_{{ ds_nodash }}_schema.json 

cat {{ params.export_table }}_{{ ds_nodash }}_schema.json
        
bq --project {{ params.gcp_project }} --location={{ params.bq_location }} load --replace  \
        --source_format=CSV {{ params.stage_dataset }}.{{ params.stage_table }}_{{ ds_nodash }} \
        gs://{{ params.export_bucket }}/{{ params.export_table }}_{{ ds_nodash }} {{ params.export_table }}_{{ ds_nodash }}_schema.json
    """,
        dag=dag)

    import_script.doc_md = """\
    #### Import table from storage to bigquery
    task documentation    
    """

    return import_script


def gen_fix_null_hack(table_config):
    """
    How silly it is that google has had this bug for months!

    Found a script in this tracker that has been modified slightly to work here.
    https://issuetracker.google.com/issues/64579566 post #22
    Changed sed to produce "" instead of N

    UPDATE:
    also fixes invalid '\r' export.  if this character is encountered it will export '\r"' breaking the csv,
    This process also patches this to become just '\\r'

    Once google fixes the export we should not need this function anymore.
    :param table_config:
    :return:
    """
    import_script = BashOperator(
        task_id='fix_csv_broken_null_values_{}'.format(table_config.params['export_table']),
        params=table_config.params,
        bash_command="""
gsutil cp gs://{{ params.export_bucket }}/{{ params.export_table }}_{{ ds_nodash }} - \
| sed 's/,"N,/,"",/g' | sed 's/,"N,/,"",/g' | sed 's/^"N,/"",/g' | sed 's/,"N$/,""/g' | sed 's/\\r"$/\\\\r/' \
| gsutil cp - gs://{{ params.export_bucket }}/{{ params.export_table }}_{{ ds_nodash }}
        """,
        dag=dag)

    import_script.doc_md = """\
        #### Fix broken NULL values in csv export because google dont know how to do it.
        task documentation    
        """

    return import_script


last_export = None
for table_config in get_tables():
    export_script = gen_export_table_task(table_config)
    export_schema_script = gen_export_schema_task(table_config)
    bq_schema_task = gen_create_bigquery_schema_from_export(table_config)
    import_script = gen_import_table_task(table_config)
    nullfix_script = gen_fix_null_hack(table_config)

    start_task >> export_schema_script >> export_script >> nullfix_script >> import_script >> end_task
    export_schema_script >> bq_schema_task >> import_script

    # only one export can run at a time
    if last_export:
        last_export >> export_schema_script
    last_export = export_script
