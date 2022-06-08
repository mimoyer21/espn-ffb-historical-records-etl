# Dag for running the GCP version of the JKL fantasy football historical records pipeline

import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

airflow_path = os.environ.get('AIRFLOW_HOME') or '~/airflow' # ~/airflow is default path airflow uses when AIRFLOW_HOME env var is not set
# airflow_path = '/Users/mmoyer/git/espn_ffb_historical_records_etl/airflow_workspace'

default_args = {
  'owner': 'mmoyer',
  'start_date': days_ago(2),
  'schedule_interval': '0 0 * * 4', # every Thursday at 0:00UTC
  'email': ['mimoyer21@gmail.com'],
  'email_on_success': True,
  'email_on_failure': True
}

ffb_gcp_dag = DAG(dag_id='ffb_gcp_dag', 
                  default_args=default_args,
                  catchup = False
                 )

start_execution = DummyOperator(task_id='start_execution', dag=ffb_gcp_dag)

# ffb_gcp.py gets data from ESPN API, transforms/aggregates, writes to CSV, and uploads to GCP Cloud Storage and BigQuery
run_python_script = BashOperator(
    task_id='run_python_script',
    # Define the bash_command
    bash_command='python ' + airflow_path + '/src/ffb_gcp.py' + ' ' + airflow_path,
    # Add the task to the dag
    dag=ffb_gcp_dag
)

email_result_csv = EmailOperator(
    task_id='email_result_csv',
    to='mimoyer21@gmail.com',
    #subject='JKL Historical Records - {{ ds }}',
    subject='JKL Historical Records',
    html_content='Attached is the CSV file with historical records.',
    files=[airflow_path + '/output_files/JKL_historical.csv'],
    dag=ffb_gcp_dag
)

end_execution = DummyOperator(task_id='end_execution', dag=ffb_gcp_dag)

# Set the order of tasks
start_execution >> run_python_script >> email_result_csv >> end_execution

