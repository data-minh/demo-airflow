from datetime import timedelta, datetime

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0,
}
# [END default_args]

# [START instantiate_dag]

with DAG(
    dag_id='spark_pi',
    start_date=days_ago(1),
    default_args=default_args,
    schedule=None,
    tags=['example']
) as dag:
    spark_pi_task = SparkKubernetesOperator(
        task_id='spark_example',
        namespace='pm-airflow',
        # relative path to DAG file
        # (1)
        application_file='/opt/airflow/dags/repo/spark/sample-spark-pi.yaml',
        # (2)
        kubernetes_conn_id='k8s',
        # (3)
        # do_xcom_push=True,
    )
    spark_pi_task