from airflow.sdk import dag, task, Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

@dag(
    dag_id='gcp_setup_dag',
    tags=["Amazon_sales"]
)
def gcp_setup_dag():

    project_id = Variable.get('GCP_PROJECT_ID')
    location = Variable.get('GCP_LOCATION')
    bucket_name = Variable.get('GCP_BUCKET_NAME')
    dataset_name = Variable.get('BQ_DATASET_NAME')

    @task.python
    def create_gcp_bucket():
        gcp_hook = GCSHook(gcp_conn_id=Variable.get('GCP_CONN_ID'))
        gcp_hook.create_bucket(
            bucket_name=bucket_name, 
            project_id=project_id, 
            location=location, 
            storage_class='REGIONAL'
        )

    @task.python
    def create_gcp_dataset():
        bq_hook = BigQueryHook(gcp_conn_id=Variable.get('GCP_CONN_ID'))
        bq_hook.create_empty_dataset(
            dataset_id=dataset_name,
            project_id=project_id,
            location=location,
            exists_ok=True # Tương đương lệnh ifExists: SKIP của Kestra
        )

    # create_gcp_bucket() >> 
    create_gcp_dataset()

gcp_setup_dag()