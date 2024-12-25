from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
from io import BytesIO

# Hàm để ghi nội dung file vào MinIO
def upload_to_minio(file_name, file_content):
    # Cấu hình kết nối đến MinIO
    minio_client = Minio(
        "192.168.1.18:32023",  # Địa chỉ MinIO
        access_key="huonganh",  # Access key
        secret_key="huonganh",  # Secret key
        secure=False  # MinIO không sử dụng HTTPS (secure=False)
    )

    bucket_name = "minh-test"

    try:
        # Kiểm tra bucket tồn tại, nếu không thì tạo mới
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        # Upload nội dung trực tiếp lên bucket
        file_data = BytesIO(file_content.encode("utf-8"))
        minio_client.put_object(
            bucket_name,
            file_name,
            file_data,
            length=len(file_content)
        )
        print(f"File '{file_name}' uploaded successfully.")

    except Exception as e:
        print(f"Error: {e}")

# Định nghĩa DAG
with DAG(
    dag_id="upload_files_to_minio",
    default_args={"start_date": datetime(2024, 12, 25)},
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Upload file 1
    task1 = PythonOperator(
        task_id="upload_file_1",
        python_callable=upload_to_minio,
        op_args=["file1.txt", "Đây là nội dung file 1."]
    )

    # Task 2: Upload file 2
    task2 = PythonOperator(
        task_id="upload_file_2",
        python_callable=upload_to_minio,
        op_args=["file2.txt", "Đây là nội dung file 2."]
    )

    # Task 3: Upload file 3
    task3 = PythonOperator(
        task_id="upload_file_3",
        python_callable=upload_to_minio,
        op_args=["file3.txt", "Đây là nội dung file 3."]
    )

    # Thiết lập thứ tự chạy các task
    task1 >> task2 >> task3