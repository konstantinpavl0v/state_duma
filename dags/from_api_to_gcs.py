import json
import requests

from airflow import DAG
from airflow.sdk import Connection, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pendulum

API_CONNECTION = "state_duma_api"
API_KEY = Variable.get("state_duma_api_key")
APP_KEY = Variable.get("state_duma_app_key")

GCS_CONNECTION = "google_cloud_default"
GCS_PROJECT = Variable.get("state_duma_gcs_project")
GCS_BUCKET = Variable.get("state_duma_gcs_bucket")


def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].strftime("%Y-%m-%d")
    end_date = context["data_interval_end"].strftime("%Y-%m-%d")

    return start_date, end_date


def _get_save_questions(**context):
    start_date, end_date = get_dates(**context)

    hook = GCSHook(gcp_conn_id=GCS_CONNECTION)

    duma_conn = Connection.get(conn_id=API_CONNECTION)
    url = f"http://{duma_conn.host}/{API_KEY}/questions.json"
    page_number = 1

    while True:
        params = {
            "dateFrom": start_date,
            "dateTo": end_date,
            "page": page_number,
            "app_token": APP_KEY
        }
        response = requests.get(url=url, params=params)
        response.raise_for_status()
        response = response.json()
        if not response["questions"]:
            break

        # upload every page as a separate file to the gcs
        hook.upload(bucket_name=GCS_BUCKET,
                    object_name=f"{start_date}_{end_date}_{page_number}.json",
                    data=json.dumps(response),
                    user_project=GCS_PROJECT,
                    mime_type="application/json")

        page_number += 1


with DAG(
        dag_id="from_api_to_gcs",
        start_date=pendulum.datetime(2025, 6, 8, 1),
        end_date=pendulum.datetime(2025, 6, 10, 1),
        schedule="@daily",
        catchup=True,
        max_active_tasks=1,
        max_active_runs=1,
        tags=["gcs", "bq"]
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    get_save_questions = PythonOperator(
        task_id="get_save_questions",
        python_callable=_get_save_questions
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_save_questions >> end
