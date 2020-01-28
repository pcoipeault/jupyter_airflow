from datetime import datetime, timedelta
import tempfile
import os

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 28, 9, 30),
    'log_response': True,
    'xcom_push': True,
    'provide_context': True,
    'execution_timeout': timedelta(minutes=5),
    'sla': timedelta(minutes=15),
    'email': ['my-email@domain.com'],
    'email_on_failure': True,
    'bucket': 'my-gcs-bucket',
    'object': 'inventory_enriched.csv',
    'database': 'lego',
    'schema': 'lego_schema',
    'postgres_conn_id': 'psql_lego',
    'gcp_conn_id': 'my_google_cloud_storage'}

ssh_command = '{{ next_ds }}'

with DAG('lego_dag',
         schedule_interval="15 10 10 * *",
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:
    def gcs_to_psql_import(**kwargs):
        fd, tmp_filename = tempfile.mkstemp(text=True)

        # download file locally
        gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=kwargs['gcp_conn_id'])
        gcs_hook.download(bucket=kwargs['bucket'], object=kwargs['object'], filename=tmp_filename)
        del gcs_hook

        # load the file into postgres
        pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['database'])
        pg_hook.bulk_load('{schema}.{table}'.format(schema=kwargs['schema'], table=kwargs['table']), tmp_filename)

        # output errors
        for output in pg_hook.conn.notices:
            print(output)

        # remove temp file
        os.close(fd)
        os.unlink(tmp_filename)

    get_files = SSHOperator(task_id='lego_get_files',
                            ssh_conn_id='ssh_jupyter_lego_getfiles',
                            command=ssh_command,
                            do_xcom_push=True)

    import_file = PythonOperator(task_id='lego_import_file',
                                 python_callable=gcs_to_psql_import,
                                 op_kwargs=default_args,
                                 provide_context=True)

    get_files >> import_file
