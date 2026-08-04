from airflow.sdk import dag, task, Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator

@dag(
    dag_id="sales_orchestrate",
    schedule=None,
    tags=["Amazon_sales"]
)
def sales_orchestrate():
    # # ingest sales data from local to GCS
    # @task.python
    # def upload_sales_data():
    #     print("Uploading sales data...")

    #     file_name = "Amazon.csv"
    #     local_file_path = f"/opt/data/raw/{file_name}"
    #     dst_file_path = f"bronze/{file_name}"
    #     gcs_bucket_name = Variable.get("GCP_BUCKET_NAME")
    #     gcp_conn_id = Variable.get("GCP_CONN_ID")

    #     upload_operator = LocalFilesystemToGCSOperator(
    #         task_id="upload_sales_data_to_gcs",
    #         src=local_file_path,
    #         dst=dst_file_path,
    #         bucket=gcs_bucket_name,
    #         gcp_conn_id=gcp_conn_id
    #     )

    #     upload_operator.execute(context={})

    # # run pyspark script to clean data and save to silver layer
    # pyspark_clean_to_silver = SSHOperator(
    #     task_id="run_pyspark_script",
    #     ssh_conn_id=Variable.get("SSH_CONN_ID"),
    #     command=(
    #         'cd /d "D:/Documents/coding_stuff/python_nerd/pipeline/spark/" && '
    #         'C:/Users/ADMIN/.local/bin/uv.exe run python spark_gcs.py'
    #     ),
    #     cmd_timeout=3600
    # )

    # dbt run to transform data from silver to gold layer
    @task.bash
    def clean_target():
        return "rm -rf /opt/dbt/target && rm -rf /opt/dbt/logs"

    @task.bash
    def source_freshness():
        return "cd /opt/dbt && dbt source freshness"

    silver_staging = BashOperator(
        task_id="dbt_silver_staging",
        cwd="/opt/dbt",
        bash_command="dbt run --select staging"
    )

    silver_staging_test = BashOperator(
        task_id="dbt_silver_staging_test",
        cwd="/opt/dbt",
        bash_command="dbt test --select staging"
    )

    gold_marts = BashOperator(
        task_id="dbt_gold_marts",
        cwd="/opt/dbt",
        bash_command="dbt run --select marts"
    )

    # upload_sales_data() >> pyspark_clean_to_silver >> 
    clean_target() >> source_freshness() >> silver_staging >> silver_staging_test >> gold_marts

sales_orchestrate()