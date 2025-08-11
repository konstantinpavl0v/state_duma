import json
import requests

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook

API_CONNECTION = "state_duma_api"
API_KEY = Variable.get("state_duma_api_key")
APP_KEY = Variable.get("state_duma_app_key")


def _get_questions(**context):
    duma_conn = Connection.get(conn_id=API_CONNECTION)
    url = f"http://{duma_conn.host}/{API_KEY}/questions.json"
    page_number = 1

    while True:
        params = {
            "dateFrom": "2025-06-09",
            "dateTo": "2025-06-10",
            "page": page_number,
            "app_token": APP_KEY
        }
        response = requests.get(url=url, params=params)
        response.raise_for_status()
        response = response.json()
        if not response["questions"]:
            break
        print(response)
        page_number += 1

def check_gcs_connection():
    """
    Проверяет, что GCS hook может успешно подключиться.
    """
    try:
        hook = GCSHook(gcp_conn_id='google_cloud_default')
        # В старых версиях можно было использовать list_blobs
        # с пустым бакетом для проверки соединения
        # (это не проверит наличие бакетов, но проверит соединение)
        bucket = hook.get_bucket("state_duma")
        print(f"Successfully connected to GCS and listed blobs in your-test-bucket.")
        return True
    except Exception as e:
        print(f"Failed to connect to GCS: {e}")
        raise

with DAG(
        dag_id="meeting_questions",
        schedule=None,
        catchup=False
):
    start = EmptyOperator(
        task_id="start",
    )

    get_questions = PythonOperator(
        task_id="get_questions",
        python_callable=_get_questions
    )

    check_connection_task = PythonOperator(
        task_id='check_gcs_connection_task',
        python_callable=check_gcs_connection,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_questions >> check_connection_task >> end
