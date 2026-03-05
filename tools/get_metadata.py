import sys

from google.cloud import firestore

from app_logging import get_logger
from constants import COLLECTION_NAME, PROJECT_ID


logger = get_logger(__name__)


def get_metadata(
    firestore_id: str,
    project_id: str | None = None,
    collection_name: str | None = None,
) -> dict:
    project = project_id or PROJECT_ID
    collection = collection_name or COLLECTION_NAME

    db = firestore.Client(project=project)
    doc_ref = db.collection(collection).document(firestore_id)
    doc = doc_ref.get()

    if not doc.exists:
        logger.error(f"FIRESTORE_ID {firestore_id} not found")
        sys.exit(1)

    data = doc.to_dict() or {}

    firestore_onpremise_table_name = data.get("01_onpremise_table_name")
    firestore_databricks_table_name = data.get("02_databricks_table_name")
    firestore_system_origin = data.get("03_system_origin")
    firestore_etl_query = data.get("04_etl_query")
    firestore_page_size_raw = data.get("05_page_size")
    firestore_delay_raw = data.get("06_delay", 1)

    firestore_page_size = int(firestore_page_size_raw) if firestore_page_size_raw is not None else None

    try:
        firestore_delay = int(firestore_delay_raw)
    except (ValueError, TypeError):
        firestore_delay = 1

    return {
        "onpremise_table_name": firestore_onpremise_table_name,
        "databricks_table_name": firestore_databricks_table_name,
        "system_origin": firestore_system_origin,
        "etl_query": firestore_etl_query,
        "page_size": firestore_page_size,
        "delay": firestore_delay,
    }
