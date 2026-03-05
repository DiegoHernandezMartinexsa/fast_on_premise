from time import strftime
import os
from uuid import uuid4
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

DATABRICKS_BUCKET_NAME = os.getenv("GCS_LANDING_BUCKET", "icasa-landing-dev")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "icasa-datalake-dev")
COLLECTION_NAME = "sync-config"
MAX_WORKERS = min(32, (os.cpu_count() or 1) + 4)
MAX_HISTORY_RANGES = 16
mode_env = os.getenv('MODE', 'DEV').upper()
PATH_EXTRACTION = f'on_demand_{mode_env.lower()}' if mode_env != 'PRD' else 'on_demand'
CURRENT_TIME = strftime("%Y%m%d%H%M%S")
METRICS_BUCKET_NAME = DATABRICKS_BUCKET_NAME
AERUNID = os.getenv("CLOUD_RUN_EXECUTION") or os.getenv("AERUNID") or str(uuid4())
AEDATTM = datetime.now(timezone.utc)
OPFLAG_VALUE = None