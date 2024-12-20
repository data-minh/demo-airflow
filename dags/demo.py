from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Function to print start message
def start_task():
    print("Starting the DAG...")

# Function to perform a basic computation
def compute_task():
    result = 1 + 1
    print(f"Computation result: {result}")
    return result

# Function to print end message
def end_task():
    print("DAG has completed successfully!")

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 20),
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id="test_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually
    catchup=False,
    tags=["test"],
) as dag:

    # Define tasks
    start = PythonOperator(
        task_id="start_task",
        python_callable=start_task,
    )

    compute = PythonOperator(
        task_id="compute_task",
        python_callable=compute_task,
    )

    end = PythonOperator(
        task_id="end_task",
        python_callable=end_task,
    )

    # Define task dependencies
    start >> compute >> end
