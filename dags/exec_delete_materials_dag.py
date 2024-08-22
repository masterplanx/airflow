import base64
import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import plentyservice
import pytz
import requests
from botocore.exceptions import ClientError
from pytz import timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

# Define DAGs
default_args = {
    'owner':'Software Team',
    'owner_links': { 'Software Team':'https://plentyag.atlassian.net/wiki/spaces/EN/pages/2379513905/Delete+Materials+Airflow+DAG+runbook'},
    'depends_on_past': False,
    'email': ['pmisra@plenty.ag'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'delete_materials',
    default_args=default_args,
    description='Delete Materials from exec service',
    schedule_interval=timedelta(hours=24),
    start_date=datetime(2023, 6, 13, 2, 0),
    catchup=False,  # Prevent Airflow from running all jobs between start_date and current date
    tags=['Executive Service','FarmOS', 'Software Team'],
)


def get_secret(secret_name: str) -> dict:
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as error:
        raise error

    secret = get_secret_value_response["SecretString"]
    try:
        secret = json.loads(secret)
    except json.decoder.JSONDecodeError as _:
        logger.error("Not a problem, using the secret before decoding")

    return secret


def load_key_from_secret_stash(secret_name):
    secret_id = os.environ.get(secret_name)
    logger.info(f"secret_id = {secret_id}")
    return get_secret(secret_id)


def get_headers():
    api_keys = load_key_from_secret_stash("PLENTY_SECRET")
    api_key = api_keys.get("PLENTY_API_KEY")
    api_secret = api_keys.get("PLENTY_API_SECRET")
    usr_pass = f"{api_key}:{api_secret}"
    b64_val = base64.b64encode(usr_pass.encode()).decode()
    headers = {
        "Authorization": f"Basic {b64_val}",
        "Content-type": "application/json; charset=utf-8",
    }
    return headers


def get_3_days_before_date_in_pdt():
    date_format = "%Y-%m-%dT%H:%M:%S.%f"
    date = datetime.now(tz=pytz.utc) - timedelta(days=3)
    date = date.astimezone(timezone("US/Pacific"))
    date_str = date.strftime(date_format)
    date_str = date_str[:-3] + "Z"
    return date_str


def call_delete_material(material_type, path):
    environment = plentyservice.common.Cfg.get_environment_context()
    domain = plentyservice.common.get_domain_from_environment(environment)
    url = f"https://executive-service.{domain}/api/v1/farm/state/delete-materials"
    logger.info(f"final url: {url}")
    headers = get_headers()
    body = {
        "materialTypes": [
            material_type
        ],
        "maybeBeforeDate": get_3_days_before_date_in_pdt(),
        "maybeAtPath": path
        }
    logging.info(f"headers: {headers}")
    logging.info(f"body: {body}")
    response = requests.post(
            url, json=body, headers=headers, timeout=10
        )
    logger.info(f"response from delete material{response.text}")


def delete_materials(material_type, path):
    """Delete materials from executive service """

    try:
        call_delete_material(material_type, path)
    except Exception as e:
        logger.info(f'Unexpected error. {e}')
        raise
    logger.info("Executed successffuly")


task_start = BashOperator(task_id='start', bash_command='date')

task_delete_blend = PythonOperator(task_id='delete_blend_lots',
                                   python_callable=delete_materials,
                                   op_args=["BLEND_LOT", "sites/LAX1/areas/PrimaryPostHarvest/lines/Conditioning"],
                                   retries=1,
                                   dag=dag)

task_delete_finished_good_components = PythonOperator(task_id='delete_finished_good_components',
                                                      python_callable=delete_materials,
                                                      op_args=["FINISHED_GOOD_COMPONENTS", "sites/LAX1/areas/PrimaryPostHarvest/lines/Packaging"],
                                                      retries=1,
                                                      dag=dag)

task_delete_finished_good = PythonOperator(task_id='delete_finished_good',
                                           python_callable=delete_materials,
                                           op_args=["FINISHED_GOOD", "sites/LAX1/areas/SecondaryPostHarvest/lines/CaseProcessing/machines/CaseFiller"],
                                           retries=1,
                                           dag=dag)

task_delete_shippable_good = PythonOperator(task_id='delete_shippable_good',
                                            python_callable=delete_materials,
                                            op_args=["SHIPPABLE_GOOD", "sites/LAX1/areas/SecondaryPostHarvest/lines/CaseProcessing/machines/Palletizer"],
                                            retries=1,
                                            dag=dag)

# Task Scheduling. Run tasks in sequence after task start.
task_start >> task_delete_blend >> task_delete_finished_good_components >> task_delete_finished_good >> task_delete_shippable_good
