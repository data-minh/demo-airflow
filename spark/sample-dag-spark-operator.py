from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

default_args = {
    'depends_on_past': False,
    'email': ['abcd@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    'sample-dag-spark-operator',
    default_args=default_args,
    description='simple dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 17),
    catchup=False,
    tags=['example'],
    template_searchpath='/opt/airflow/dags/repo/spark'
) as dag:
    t1 = SparkKubernetesOperator(
        task_id='n-spark-pi',
        trigger_rule="all_success",
        depends_on_past=False,
        application_file="sample-spark-pi.yaml",
        namespace="pm-spark",
        kubernetes_conn_id="k8s",
        do_xcom_push=True,
        dag=dag
    )
    t1.template_ext = ()
