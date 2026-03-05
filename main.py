import os
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv

from app_logging import get_logger
from tools.get_metadata import get_metadata
from tools.get_user_info import get_user_info
from tools.casting_query import cast_query
from tools.extract import extract_data, prepare_query_for_extraction
from tools.load import load_data
from tools.observability import Observability
from constants import MAX_WORKERS


def main() -> None:
    load_dotenv()
    logger = get_logger(__name__)
    
    # Initialize Observability
    # Use CLOUD_RUN_EXECUTION as job_id if available, else manual_run
    job_id = os.getenv("CLOUD_RUN_EXECUTION") or "manual_run"
    # We need table_name for Observability, but we don't have it yet.
    # We'll initialize it with a placeholder and update it later.
    observability = Observability(table_name="unknown", job_id=job_id, extraction_type="unknown")

    try:
        # First check arguments to get firestore_id
        import sys
        if len(sys.argv) < 2:
            logger.error("FIRESTORE_ID is required")
            sys.exit(1)
            
        firestore_id = sys.argv[1]
        
        # Load metadata first to get delay
        metadata = get_metadata(firestore_id)
        delay = metadata.get("delay", 1)

        # Get user info, passing the delay for default date calculation
        user_info = get_user_info(delay=delay)
        
        init_date = user_info["init_date"]
        end_date = user_info["end_date"]
        extraction_type = user_info["extraction_type"]
        
        # Inyeccion del entorno en metadata para load_data
        metadata['target_project'] = user_info['target_project']
        metadata['target_bucket'] = user_info['target_bucket']
        metadata['mode'] = user_info['mode'] # 'dev', 'qa' o 'prd'

        # Update Observability context
        observability.extraction_type = extraction_type
        observability.table_name = metadata['databricks_table_name']

        ### onpremise_tablename = metadata["onpremise_table_name"]
        ### databricks_tablename = metadata['databricks_table_name']
        
        # Update Observability table_name
        ### observability.table_name = databricks_tablename
        logger.info(f"Targeting Project: {metadata['target_project']} (Mode: {metadata['mode']})")
        logger.info(f"Metadata loaded for {firestore_id}")
        logger.debug(f"User date range: {init_date} - {end_date}")
        logger.debug(f"CLOUD_RUN_JOB: {os.getenv('CLOUD_RUN_JOB')}")
        logger.debug(f"CLOUD_RUN_EXECUTION: {os.getenv('CLOUD_RUN_EXECUTION')}")
        logger.debug(f"Extraction type: {extraction_type}")
        logger.debug(f"Metadata: {metadata}")

        if metadata['mode'].upper() == 'DEV':
            page_size = int(os.getenv("PAGE_SIZE", 10000))
        else:
            page_size = int(metadata.get('page_size',100000))

        # 1. Cast columns to VARCHAR
        etl_query = metadata['etl_query']
        casted_etl_query = cast_query(etl_query, metadata["onpremise_table_name"])

        # 2. Prepare query based on extraction type (FULL/CDC)
        final_query = prepare_query_for_extraction(casted_etl_query, extraction_type)
        logger.info(f"Final query prepared for {extraction_type} extraction")

        # 3. Extract and load data in parallel
        logger.info(f"Starting extraction with page_size={page_size}, max_workers={MAX_WORKERS}")
        logger.info(f"Starting parallel extraction/load into {metadata['target_bucket']}")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            
            # Iterate over the generator from extract_data
            for i, df_chunk in enumerate(extract_data(final_query, page_size, init_date, end_date)):
                logger.info(f"Extracted chunk {i} with {df_chunk.height} rows")
                future = executor.submit(load_data, df_chunk, metadata, extraction_type, i)
                futures.append(future)
                
            logger.info(f"All {len(futures)} chunks submitted. Waiting for completion...")
            
            # Collect metrics from futures
            for future in futures:
                rows_count, size_mb = future.result()
                observability.update_metrics(rows_count, size_mb)

        #Print env variables CLOUD_RUN_JOB and CLOUD_RUN_EXECUTION
        logger.info(f"CLOUD_RUN_JOB: {os.getenv('CLOUD_RUN_JOB')}")
        logger.info(f"CLOUD_RUN_EXECUTION: {os.getenv('CLOUD_RUN_EXECUTION')}")
        logger.info(f"Extraction and loading finished {extraction_type} extraction ")
        
        observability.set_success()

    except Exception as e:
        logger.error(f"ETL Failed: {e}")
        observability.set_error(e)
        raise e
        
    finally:
        # Save metrics to GCS
        observability.save_metrics()



if __name__ == "__main__":
    main()


