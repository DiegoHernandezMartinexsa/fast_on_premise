import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

from google.cloud import storage

from constants import METRICS_BUCKET_NAME
from app_logging import get_logger

logger = get_logger(__name__)

class Observability:
    def __init__(self, table_name: str, job_id: str, extraction_type: str):
        self.table_name = table_name
        self.job_id = job_id
        self.extraction_type = extraction_type
        self.start_time = datetime.utcnow()  
        self.rows_read = 0
        self.rows_written = 0
        self.data_size_mb = 0.0
        self.status = "RUNNING"
        self.error_message: Optional[str] = None
        self.error_type: Optional[str] = None
        self.source_endpoint = "sql-server-dataanalytics"  # Can be dynamic if needed
        self.target_path = f"gs://{METRICS_BUCKET_NAME}/metrics/table={table_name}/"

    def update_metrics(self, rows: int, size_mb: float):
        self.rows_read += rows
        self.rows_written += rows
        self.data_size_mb += size_mb

    def set_error(self, error: Exception):
        self.status = "FAILED"
        self.error_type = type(error).__name__
        self.error_message = str(error)

    def set_success(self):
        self.status = "SUCCESS"

    def _calculate_duration(self) -> float:
        return (datetime.utcnow() - self.start_time).total_seconds()

    def _generate_report(self) -> Dict[str, Any]:
        end_time = datetime.utcnow()
        duration = self._calculate_duration()
        throughput = self.rows_written / duration if duration > 0 else 0.0

        return {
            "metadata": {
                "job_id": self.job_id,
                "table_name": self.table_name,
                "status": self.status,
                "execution_date": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "extraction_type": self.extraction_type
            },
            "metrics": {
                "rows_read": self.rows_read,
                "rows_written": self.rows_written,
                "data_size_mb": round(self.data_size_mb, 4),
                "duration_seconds": round(duration, 4),
                "throughput_rows_per_second": round(throughput, 2)
            },
            "timestamps": {
                "start_time": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            },
            "lineage": {
                "source_endpoint": self.source_endpoint,
                "target_path": self.target_path
            },
            "error_details": {
                "has_error": self.status == "FAILED",
                "error_type": self.error_type,
                "error_message": self.error_message
            }
        }

    def save_metrics(self):
        # Check if running in Cloud Run environment
        cloud_run_job = os.getenv("CLOUD_RUN_JOB")
        cloud_run_exec = os.getenv("CLOUD_RUN_EXECUTION")
        
        if not cloud_run_job and not cloud_run_exec:
            logger.info("Running locally. Skipping metrics upload to GCS.")
            # Optionally print the JSON for debugging
            logger.debug(f"Metrics Report: {json.dumps(self._generate_report(), indent=2)}")
            return

        report = self._generate_report()
        report_json = json.dumps(report)
        
        # Partitioned path: metrics/table={table_name}/year={yyyy}/month={mm}/day={dd}/{job_id}.json
        date_now = datetime.utcnow()
        blob_path = (
            f"table={self.table_name}/"
            f"year={date_now.year}/"
            f"month={date_now.month:02d}/"
            f"day={date_now.day:02d}/"
            f"{self.job_id}.json"
        )

        try:
            client = storage.Client()
            bucket = client.bucket(METRICS_BUCKET_NAME)
            blob = bucket.blob(blob_path)
            blob.upload_from_string(report_json, content_type="application/json")
            logger.info(f"Metrics report uploaded successfully to gs://{METRICS_BUCKET_NAME}/{blob_path}")
        except Exception as e:
            logger.error(f"Failed to upload metrics report: {e}")
