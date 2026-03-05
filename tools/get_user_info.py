import sys
import os
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any

from app_logging import get_logger


logger = get_logger(__name__)


def _parse_date(value: str, label: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        logger.error(f"{label} must have format YYYY-MM-DD, got {value!r}")
        sys.exit(1)


def get_user_info(delay: int = 1) -> Dict[str, Any]:
    num_args = len(sys.argv)

    if num_args < 2:
        logger.error("FIRESTORE_ID is required")
        sys.exit(1)

    firestore_id = sys.argv[1]

    init_date: Optional[str] = None
    end_date: Optional[str] = None
    extraction_type: str = os.getenv("TYPE_EXTRACTION", "cdc")
    target_env: str = os.getenv("TARGET_ENV", "dev").lower()

    if num_args >= 6:
        init_date = sys.argv[2]
        end_date = sys.argv[3]
        extraction_type = sys.argv[4]
        target_env = sys.argv[5].lower()
    if num_args == 5:
        init_date = sys.argv[2]
        end_date = sys.argv[3]
        extraction_type = sys.argv[4]
    elif num_args == 4:
        init_date = sys.argv[2]
        end_date = sys.argv[3]
    elif num_args > 6:
        logger.error("Usage: main.py FIRESTORE_ID [INIT_DATE END_DATE [EXTRACTION_TYPE [TARGET_ENV]]]")
        sys.exit(1)

    if init_date is None or end_date is None:
        today = date.today()
        # end_date = current_date - delay
        end_dt = today - timedelta(days=delay)
        # init_date = end_date - 31 days
        init_dt = end_dt - timedelta(days=31)
        
        init_date = init_dt.strftime("%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")

    # Validate date format and order
    init_dt_obj = _parse_date(init_date, "init_date")
    end_dt_obj = _parse_date(end_date, "end_date")

    if init_dt_obj >= end_dt_obj:
        logger.error(f"init_date ({init_date}) must be less than end_date ({end_date})")
        sys.exit(1)

    if extraction_type not in ("full", "cdc"):
        logger.error(f"extraction_type must be 'full' or 'cdc', got {extraction_type!r}")
        sys.exit(1)

    # --- Configuracion Dinamica de Proyectos y Buckets ---
    env_mapping = {
        "dev": {
            "project_id": "icasa-datalake-dev",
            "bucket_name": "icasa-landing-dev"
        },
        "qa": {
            "project_id": "icasa-datalake-qa",
            "bucket_name": "icasa-landing-qa"
        },
        "prd": {
            "project_id": "icasa-datalake-prd",
            "bucket_name": "icasa-landing-prd"
        }
    }

    config = env_mapping.get(target_env)
    if not config:
        logger.error(f"Invalid environment: {target_env}. Must be 'dev', 'qa' or 'prd'")
        sys.exit(1)

    logger.info(f"Target Environment set to: {target_env.upper()}")
    logger.info(f"Target Project: {config['project_id']}")
    logger.info(f"Target Bucket: {config['bucket_name']}")

    return {
        "firestore_id": firestore_id,
        "init_date": init_date,
        "end_date": end_date,
        "extraction_type": extraction_type,
        "target_project": config["project_id"],
        "target_bucket": config["bucket_name"],
        "mode": target_env
    }