from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'email': ['abcd@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'sample-dag-spark-operator',
    default_args=default_args,
    description='A sample DAG to run Spark on Kubernetes',
    schedule_interval=None,
    start_date=datetime(2025, 3, 20),
    catchup=False,
    template_searchpath='/opt/airflow/dags/repo/spark/job',
) as dag:

    n_spark_pi_scala = SparkKubernetesOperator(
        task_id='n-spark-pi-scala',
        namespace='pm-spark',
        application_file='sample-spark-pi.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=True
    )

    n_spark_pi_python = SparkKubernetesOperator(
        task_id='n-spark-pi-python',
        namespace='pm-spark',
        application_file='spark-pi-python.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=True
    )
    n_spark_pi_scala >> n_spark_pi_python