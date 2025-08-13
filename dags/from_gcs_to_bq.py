import json

from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import Variable
import pendulum

GCS_CONNECTION = "google_cloud_default"
GCS_PROJECT = Variable.get("state_duma_gcs_project")
GCS_BUCKET = Variable.get("state_duma_gcs_bucket")
GCS_DATASET = Variable.get("state_duma_bq_dataset")
GCS_TABLE = Variable.get("state_duma_bq_table")


def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].strftime("%Y-%m-%d")
    end_date = context["data_interval_end"].strftime("%Y-%m-%d")

    return start_date, end_date


def _get_save_questions(**context):
    start_date, end_date = get_dates(**context)
    hook = GCSHook(gcp_conn_id=GCS_CONNECTION)
    bq_hook = BigQueryHook(gcp_conn_id=GCS_CONNECTION, use_legacy_sql=False)

    # get objects from gcs
    objects = hook.list(bucket_name="state_duma", prefix=f"{start_date}_{end_date}")

    # check objects
    if not objects:
        print(f"No objects in for the {start_date} and {end_date}")
        return

    # delete the data from the table for the particular dates to maintain indempotentency
    query = f"""
            DELETE FROM `{GCS_PROJECT}.{GCS_DATASET}.{GCS_TABLE}`
            WHERE start_date = '{start_date}'
              AND end_date = '{end_date}'"""
    bq_hook.run(sql=query)

    # add to the data start_date and end_date, insert the data to the bq
    for object in objects:
        data = json.loads(hook.download(bucket_name="state_duma", object_name=object))
        transformed_json = []
        for question in data["questions"]:
            question["start_date"] = start_date
            question["end_date"] = end_date
            transformed_json.append(question)

        bq_hook.insert_all(project_id=GCS_PROJECT, dataset_id=GCS_DATASET, table_id=GCS_TABLE,
                           rows=transformed_json)


with DAG(
        dag_id="from_gcs_to_bq",
        start_date=pendulum.datetime(2025, 6, 8, 1),
        end_date=pendulum.datetime(2025, 6, 10, 1),
        schedule="@daily",
        catchup=True,
        max_active_tasks=1,
        max_active_runs=1,
        tags=["gcs"]
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

wait_upload_to_gcs = ExternalTaskSensor(
    task_id="wait_upload_to_gcs",
    external_dag_id="from_api_to_gcs",
    allowed_states=["success"],
    mode="reschedule",
    timeout=360000,
    poke_interval=60,
)

get_save_questions = PythonOperator(
    task_id="get_save_questions",
    python_callable=_get_save_questions
)

end = EmptyOperator(
    task_id="end",
)

start >> wait_upload_to_gcs >> get_save_questions >> end
