import os
import json
import logging
from datetime import datetime, timedelta
import pprint

from airflow import DAG
import boto3
import botocore

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task_group, task
from airflow.utils.trigger_rule import TriggerRule

from sheets_ingest_service import SheetsIngestService

logger = logging.getLogger("airflow.task")

sis = SheetsIngestService()
s3 = boto3.client("s3", region_name="us-west-2")

# EDIT
# Configure job
# ================================================
# Stitch ID for Snowflake schema
SCHEMA__PLANTSCISHEETSIMPORTER = (
    "b5a6ef0cff42527375d830e34ce3cd16b143e2512e7051bc27bc5fa3fb3bedd3"
)
# Updated PlantSci Google sheets agnostic to crop (omnicrop)
SCHEMA__PLANTSCISHEETSIMPORTER_V2 = (
    "954c18b7b4fdf8a9dd1b0029949b15a6d2f91767ed9c6c78d9320eee0d19ac3a"
)

# S3 bucket (Acct. ID: 617489010939)
# Note that this bucket ONLY exists in AWS prod account (before DevOps split AWS up into dev, staging, prod)
# https://s3.console.aws.amazon.com/s3/buckets/dev-sheets?region=us-west-2&tab=objects
BUCKET_NAME = "dev-sheets"
STATE_FILE_NAME = "state.json"

# Environment variables required for package to work (to read data from Google Drive/Sheets)
#  https://github.com/PlentyAg/sheets-ingest-service
GOOGLE_OAUTH_TOKEN = os.getenv("GOOGLE_OAUTH_TOKEN")
GOOGLE_OAUTH_REFRESH_TOKEN = os.getenv("GOOGLE_OAUTH_REFRESH_TOKEN")
GOOGLE_OAUTH_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
# Credentials required to persist ingest state in S3 bucket
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

if None in [
    GOOGLE_OAUTH_TOKEN,
    GOOGLE_OAUTH_REFRESH_TOKEN,
    GOOGLE_OAUTH_CLIENT_ID,
    GOOGLE_OAUTH_CLIENT_SECRET,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
]:
    raise KeyError("All Sheets Ingest credentials not found in environment!")
# ================================================

# EDIT
# Google Sheets Ingest Configuration
# Change dictionary to add NEW sheets to ingest!
# ================================================
CONFIG = {
    "projects": {
        "THRESHOLDS": {
            "folder_id": "1rQ7sY9L1V-ilchGqU0YbXgUM3Z1rsulA",
            "stitch_id": SCHEMA__PLANTSCISHEETSIMPORTER_V2,
            "sheets": [
                "plant_yield_targets",
                "lg_yield_targets",
                "lg_qa_leaf_spec",
                "plant_tissue_thresholds",
                "plant_fertigation_thresholds",
            ],
        },
        "OMNICROPS": {
            # Data for all crops in unified Google Sheets tables
            # Folder Name: omnicrops_approved_data
            # URL: https://drive.google.com/drive/u/1/folders/1fldU8jg6CxGJwmnb6CCE4JlldmGP8UMW
            "folder_id": "1fldU8jg6CxGJwmnb6CCE4JlldmGP8UMW",
            "stitch_id": SCHEMA__PLANTSCISHEETSIMPORTER_V2,
            "sheets": [
                "treatment_database",
                "anions",
                "auto_brix",
                "auto_titrator",
                "biomass",
                "carbos",
                "cations",
                "chlorophyll_meter",
                "cid_710_device",
                "condition",
                "firmness",
                "flowering",
                "flower_phenotype",
                "flower_mapping",
                "fruit_phenotype",
                "foliar_powdery_mildew_severity",
                "fruit_development",
                "growvera",
                "handheld_brix",
                "harvest",
                "health",
                "ipm_incidence",
                "ipm_application_record",
                "leaf_dev",
                "leaf_area_index",
                "licor6800",
                "licor600",
                "meta",
                "organic_acids",
                "plant_phenotype",
                "pruning",
                "qct_meta",
                "qct_master_tracker",
                "runner_harvest",
                "seeding",
                "substrate_probe",
                "texture",
                "vegetative",
                "volatiles",
            ],
        },
        "STRW": {
            "folder_id": "1aIQsp8fsSvoJImDoHHiCSgxdudLW3Hu3",
            "stitch_id": SCHEMA__PLANTSCISHEETSIMPORTER,
            "sheets": [
                "Strw",
                "meta",
                "growth",
                "plant_health",
                "runner_harvest",
                "fruit_development",
                "harvest",
                "post_harvest_LAR",
                "root_environment",
                "leaf_development",
                "strw_brix_firmness",
                "strw_survey",
                "fruit_mass",
                "physiology",
                "postharvest_quality",
                "postharvest_quality_texture",
            ],
        },
        "LG": {
            "folder_id": "1mRgDQkQWJWHTiREAyCMRv4Xvk38nhgxb",
            "stitch_id": SCHEMA__PLANTSCISHEETSIMPORTER,
            "sheets": [
                "lg_growout",
                "shelf_life_v2",
                "shelf_life_new_format",
                "lg_shelf_life_v2",
                "lg_destructive_harvest",
                "lg_seedling_counts",
                "Prop",
                "Growout",
                "Leafy Greens",
                "Leafy Greens 2.0",
                "postharvest_quality",
                "postharvest_quality_texture",
            ],
        },
        "EXT": {
            "folder_id": "1l7lvesaRmxoyi_fLWkQLORGJj8eFar1W",
            "stitch_id": SCHEMA__PLANTSCISHEETSIMPORTER,
            "sheets": [
                "Product Testing",
            ],
        },
        "TMTO": {
            "folder_id": "1vf4MpxZKHdkLbmQKOh082F6_GnSfcfwV",
            "stitch_id": SCHEMA__PLANTSCISHEETSIMPORTER,
            "sheets": [
                "Tomato",
                "tmt_growout_brix_firmness",
                "tmt_growout_flowering_and_final_weight_tower",
                "tmt_growout_growth",
                "tmt_growout_harvest",
                "harvest",
                "meta",
                "tmt_growout_plant_health",
                "tmt_growout_shelf_life_v2",
                "root_environment",
                "leaf_development",
                "tmt_prop_emergence_count",
                "tmt_prop_flowering_data",
                "tmt_prop_meta",
                "tmt_prop_seedling_data",
                "tmt_survey",
                "postharvest_quality",
                "postharvest_quality_texture",
            ],
        },
        "NC": {
            "folder_id": "10reYutQrIF90ddZ0Qj-mopR1fQ3KzMUb",
            "stitch_id": SCHEMA__PLANTSCISHEETSIMPORTER,
            "sheets": [
                "NewCrops",
                "meta",
                "harvest",
                "postharvest_quality",
                "postharvest_quality_texture",
            ],
        },
        "TIGRIS": {
            "folder_id": "12nPx6FaD5HcVnaQlLRKOQe55ljUmSren",
            "stitch_id": "65a7c13c18ca05dea7f2559cc15770a3b8760874749ca89567a3c078c19e76e0",
            "sheets": [
                "seed_count",
                "emergence_count",
                "plug_dimensions",
                "transplant_miss",
                "harvest_biomass",
            ],
        },
        "TIGRIS_PROCESS_TRIAL_LIST": {
            "folder_id": "1PYeN_pZ-iGNvC8MbsGfxmQ77yG7V8oDs",
            "stitch_id": "65a7c13c18ca05dea7f2559cc15770a3b8760874749ca89567a3c078c19e76e0",
            "sheets": [
                "Process Team",
            ],
        },
        "FARM_OPERATIONS": {
            "folder_id": "19RoXDo3fZ0qWLUzPK3cHFQDTBO5jCx_4",
            "stitch_id": "22a23cf3a77b849bec7def40f581e107786fa7a0c99c6645c56e33a6eb77e856",
            "sheets": [
                "tigris_trays_composted",
                "tigris_manual_inputs",
                "cpt_manual_inputs",
                "euphrates_nutrient_recipe",
                "Euphrates_SeedCountMonitor",
                "compton_otif",
                "cpt_water_energy_usage_data",
                "standard_work",
                "compliance",
                "atp_failures",
                "cpt_oee_specifications",
                "CPT Pesticide Log",
                "cpt_losses",
                "Recipe Log",
                "whole_foods_nielsen_report",
            ],
        },
        "OPERATION_KPIS": {
            "folder_id": "1YOS_Kw3xc1aesLXbvoPXjghbVgIbT-Ld",
            "stitch_id": "22a23cf3a77b849bec7def40f581e107786fa7a0c99c6645c56e33a6eb77e856",
            "sheets": [
                "compton_s_n_op_forecast",
                "compton_otif",
                "LCAP Targets",
                "compton_otif_fcts",
                "Growers",
                "cpt_yield",
                "cpt_por",
                "cpt_downtime",
                "Seed Count",
                "Form Responses 1_phEUP",
                "Form Responses 3_cpEUP",
                "Form Responses 1_transplantStatus",
                "Form Responses 1_cultivationEUP",
            ],
        },
        "YIELD_FORECASTING": {
            "folder_id": "1lYtVR3_qlvCopYsldkyJEwM20vCkX1JE",
            "stitch_id": "22a23cf3a77b849bec7def40f581e107786fa7a0c99c6645c56e33a6eb77e856",
            "sheets": [
                "kgs_per_tower__model_1__weekly",
                "kgs_per_tower__model_2__weekly",
            ],
        },
        "PQP": {
            # Lab Approved Data
            # https://drive.google.com/drive/u/1/folders/1aIQsp8fsSvoJImDoHHiCSgxdudLW3Hu3
            "folder_id": "1aIQsp8fsSvoJImDoHHiCSgxdudLW3Hu3",
            "stitch_id": SCHEMA__PLANTSCISHEETSIMPORTER,
            "sheets": [
                "carbos",
                "organic_acids",
                "anions",
                "volatiles",
            ],
        },
        "LICOR": {
            # Research Tools Approved Data
            # https://drive.google.com/drive/u/1/folders/1xpSqk-0XIc-qeFxrzwk8r6yOPzBCnmBB
            "folder_id": "1xpSqk-0XIc-qeFxrzwk8r6yOPzBCnmBB",
            "stitch_id": SCHEMA__PLANTSCISHEETSIMPORTER,
            "sheets": [
                "licor6800",
                "licor600",
            ],
        },
    }
}

# ================================================

# Utils
# ================================================


def to_lower_snake_case(input_string):
    words = input_string.split()
    lower_case_words = [word.lower() for word in words]
    snake_case_string = "_".join(lower_case_words)
    return snake_case_string


def create_resource(data, key, bucket_name):
    """PUT state data in S3 bucket"""
    res = s3.put_object(Body=data, Bucket=bucket_name, Key=key)
    return res


def get_resource(key, bucket_name):
    """GET state data from S3 bucket"""
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        content = json.loads(obj["Body"].read())
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info("Resource does not exist. Will be created after job runs.")
        else:
            logger.info(f"Unexpected Error {e}")
            raise
    else:
        return content


def etl_sheets(config, state_file_name, bucket_name):
    """Run SIS. Persist state on AWS S3.

    Documentation
    https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html
    """

    try:
        state = get_resource(state_file_name, bucket_name)
    except Exception as e:
        logger.info(f"Unexpected error. {e}")
        state = None

    # Extract data from Sheets. Persist state in S3 bucket
    # https://s3.console.aws.amazon.com/s3/buckets/dev-sheets
    res, state = sis.run(config=config, state=state)
    logger.info(f"DAG Response: {res}")
    s3_res = create_resource(json.dumps(state), state_file_name, bucket_name)
    logger.info(f"S3 Response: {s3_res}")
    return {"status": "complete"}


# ================================================

# Define DAG and Tasks
# ================================================
default_args = {
    "owner": "Data Team",
    "owner_links": {
        "Data Team": "https://plentyag.atlassian.net/wiki/spaces/DATA/pages/1421578486/Sheets+Ingest+Service+V2+HLD"
    },
    "depends_on_past": False,
    "email": ["kbochanski@plenty.ag"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "sheets-ingest",
    default_args=default_args,
    description="Ingest R&D Google Sheets data",
    schedule_interval=timedelta(hours=12),
    start_date=datetime(2022, 10, 18, 2, 0),
    catchup=False,  # Prevent Airflow from running all jobs between start_date and current date
    tags=["Google Sheets Ingest Service", "Data Team", "Data Pipeline"],
    # Limit maximum sheet reads to 2 to prevent surpassing allowed requests to Google API endpoints
    max_active_tasks=2,
)

# Start and end nodes
start_task = BashOperator(task_id="start_task", bash_command="date", dag=dag)
end_task = DummyOperator(task_id="end_task", dag=dag)

# Context print task
# @task(task_id="end_task")
# def end_task(ds=None, **kwargs):
#     """Print the Airflow context and ds variable from the context."""
#     logger.info(ds)
#     logger.info(pprint.pformat(kwargs['ts']))
#     return "Done"


def download_failed():
    print("Download failed")
    raise ValueError("error")


# Iterate over each project
current_proj_group = None
for project_name, config_values in CONFIG["projects"].items():
    # Create a task group for each project
    with TaskGroup(group_id=f"task_group_{project_name}", dag=dag) as proj_group:
        # Create tasks for each sheet in the project
        # These are part of task group
        for sheet_name in config_values["sheets"]:
            # Update config to include only one sheet name per job (normally a list of sheets)
            config = {
                "projects": {
                    project_name: {
                        "folder_id": config_values["folder_id"],
                        "stitch_id": config_values["stitch_id"],
                        "sheets": [sheet_name],
                    }
                }
            }
            # Ingest sheet task.
            task_ingest_sheet = PythonOperator(
                task_id=f"sheets_ingest_{to_lower_snake_case(sheet_name)}",
                python_callable=etl_sheets,
                op_args=[
                    config,
                    f"{to_lower_snake_case(sheet_name)}_" + STATE_FILE_NAME,
                    BUCKET_NAME,
                ],
                retries=1,
                execution_timeout=None,
                trigger_rule=TriggerRule.ALL_DONE,
                dag=dag,
            )

    # Mock failed task
    # download_failed_task = PythonOperator(
    #                 task_id=f'download_fail{to_lower_snake_case(sheet_name)}',
    #                 python_callable=download_failed,
    #                 trigger_rule="all_done"
    #             )

    # Set dependencies between project task groups
    if not current_proj_group:
        start_task >> proj_group
    else:
        current_proj_group.set_downstream(proj_group)
    current_proj_group = proj_group

# Set end task
current_proj_group.set_downstream(end_task)

current_proj_group
# ================================================
