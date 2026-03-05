import time
from io import BytesIO
from constants import CURRENT_TIME
import polars as pl
from google.cloud import storage
from app_logging import get_logger
from typing import Dict, Any, Tuple

logger = get_logger(__name__)

from constants import DATABRICKS_BUCKET_NAME, CURRENT_TIME

def load_data(df: pl.DataFrame, metadata: Dict[str, Any], extraction_type: str, chunk_index: int) -> Tuple[int, float]:
    system_origin = metadata['system_origin']
    table_name = metadata['databricks_table_name']
    
    target_project = metadata.get('target_project', 'icasa-datalake-dev')
    target_bucket = metadata.get('target_bucket', 'icasa-landing-dev')
    
    table_count = df.height
    
    # Parse CURRENT_TIME (YYYYMMDDHHMMSS) for partitions
    # YYYY-MM-DD
    current_date_str = f"{CURRENT_TIME[:4]}-{CURRENT_TIME[4:6]}-{CURRENT_TIME[6:8]}"
    # HH
    current_hour_str = CURRENT_TIME[8:10]
    # YYYYMMDD
    current_date_compact = CURRENT_TIME[:8]

    # Simula nombre de archivo con timestamp y chunk_index
    base_filepath = f'{system_origin}/{table_name}/{extraction_type}/load_date={current_date_str}/load_hour={current_hour_str}'
    blob_name = f"{base_filepath}/{table_name}-{extraction_type}-{current_date_compact}-{table_count}-{chunk_index:06d}.parquet"

    logger.info(f"Databricks path: {blob_name}")
    logger.info(f"Uploading chunk {chunk_index} ({table_count} rows) to {blob_name}...")
    
    storage_client = storage.Client(project=target_project) 
    bucket = storage_client.bucket(target_bucket)         
    buffer = BytesIO() 
    df.write_parquet(buffer, compression='snappy') 
    buffer.seek(0)
    
    # Calculate size in MB
    size_mb = buffer.getbuffer().nbytes / (1024 * 1024)
         
    # 4. Subida a GCS 
    blob = bucket.blob(blob_name) 
    blob.upload_from_file(buffer, content_type='application/octet-stream')
    
    logger.info(f"Chunk {chunk_index} finished. Size: {size_mb:.2f} MB")
    return table_count, size_mb
