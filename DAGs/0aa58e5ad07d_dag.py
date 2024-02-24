from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 


#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Workspace/Users/saqub_ali88@hotmail.co.uk/0aa58e5ad07d.geo'
}

#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': 'Saqub',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


with DAG('0aa58e5ad07d_dag',
    # should be a datetime format
    start_date=datetime(2024, 2, 23),
    # check out possible intervals, should be a string
    schedule_interval=timedelta(days=1),
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
