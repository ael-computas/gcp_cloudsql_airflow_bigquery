# -*- coding: utf-8 -*-
your_db_TABLES = ["database.dbo.thetable"]

dag = DAG('your_db_to_bigquery', default_args=default_args)
start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)


def gen_filename(tbl):
    return tbl.split(".")[-1]


def gen_export_task(tbl):
    filename = gen_filename(tbl)
    export_tbl = MySqlToGoogleCloudStorageOperator(
        task_id='extract_data_from_{}'.format(filename),
        mysql_conn_id='your_db',
        google_cloud_storage_conn_id='your-connection-id',
        sql='SELECT * FROM {}'.format(tbl),
        table_schema=tbl,
        bucket='your-bucket',
        filename='your_db/tables/{}'.format(filename) + '/{}.json',
        schema_filename='your_db/schemas/{}.json'.format(filename),
        dag=dag)
    return export_tbl


def gen_import_task(filename):
    import_task = GoogleCloudStorageToBigQueryOperator(
        task_id='{}_to_bigquery'.format(filename),
        bucket='your-bucket',
        source_objects=['your_db/tables/{}/*'.format(filename)],
        destination_project_dataset_table='your-bigquery-project.your_db_raw.{}'.format(filename),
        schema_object='your_db/schemas/{}.json'.format(filename),
        write_disposition='WRITE_TRUNCATE',
        source_format="NEWLINE_DELIMITED_JSON",
        dag=dag)
    return import_task


for table in your_db_TABLES:
    export_table = gen_export_task(table)
    import_file = gen_import_task(gen_filename(table))
    start_task >> export_table >> import_file >> end_task