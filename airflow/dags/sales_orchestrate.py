from airflow.sdk import dag, task, Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

@dag(
    dag_id="sales_orchestrate",
    schedule=None,
    tags=["Amazon_sales"]
)
def sales_orchestrate():
    @task.python
    def upload_sales_data():
        print("Uploading sales data...")

        file_name = "Amazon.csv"
        local_file_path = f"/opt/data/raw/{file_name}"
        dst_file_path = f"bronze/{file_name}"
        gcs_bucket_name = Variable.get("GCP_BUCKET_NAME")
        gcp_conn_id = Variable.get("GCP_CONN_ID")

        upload_operator = LocalFilesystemToGCSOperator(
            task_id="upload_sales_data_to_gcs",
            src=local_file_path,
            dst=dst_file_path,
            bucket=gcs_bucket_name,
            gcp_conn_id=gcp_conn_id
        )

        upload_operator.execute(context={})

    @task.bash
    def pyspark_clean_to_silver():
        run_pyspark_script = SSHOperator(
            task_id="run_pyspark_script",
            ssh_conn_id=Variable.get("SSH_CONN_ID"),
            command=(
                'cd /d "D:/Documents/coding_stuff/python_nerd/pipeline/spark/" && '
                'C:/Users/ADMIN/.local/bin/uv.exe run python spark_gcs.py'
            ),
            cmd_timeout=3600
        )

        run_pyspark_script.execute(context={}) 

    upload_sales_data() >> pyspark_clean_to_silver()

sales_orchestrate()