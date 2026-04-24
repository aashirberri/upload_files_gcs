import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
from logger import setup_logger

load_dotenv()
logger = setup_logger(__name__)

SERVICE_ACCOUNT_KEY = os.getenv("SERVICE_ACCOUNT_KEY")
BUCKET_NAME         = os.getenv("BUCKET_NAME")
SOURCE_FOLDER       = os.getenv("SOURCE_FOLDER")
DESTINATION_PREFIX  = os.getenv("DESTINATION_PREFIX", "candidate-profiles")
MAX_WORKERS         = 10

if not all([SERVICE_ACCOUNT_KEY, BUCKET_NAME, SOURCE_FOLDER]):
    raise EnvironmentError("Missing required env variables. Ensure SERVICE_ACCOUNT_KEY, BUCKET_NAME, and SOURCE_FOLDER are set in .env")
if not os.path.exists(SERVICE_ACCOUNT_KEY):
    raise FileNotFoundError(f"Service account key not found at: {SERVICE_ACCOUNT_KEY}")
if not os.path.exists(SOURCE_FOLDER):
    raise FileNotFoundError(f"Source folder not found at: {SOURCE_FOLDER}")

def get_gcs_client():
    # Authenticate using the service account JSON key and return a GCS client
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_KEY,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return storage.Client(credentials=credentials)

def upload_file(bucket, local_path: Path, blob_name: str):
    # Upload a single file to the GCS bucket and return "uploaded", "skipped", or "failed"
    try:
        blob = bucket.blob(blob_name)
        if blob.exists():
            logger.warning(f"Skipped (already exists): {blob_name}")
            return "skipped"

        blob.upload_from_filename(str(local_path))
        logger.info(f"Uploaded: {local_path.name} --> gs://{bucket.name}/{blob_name}")
        return "uploaded"

    except Exception as e:
        logger.error(f"Failed to upload {local_path.name}: {e}")
        return "failed"


def upload_folder(source_folder: str, bucket_name: str, destination_prefix: str):
    # Walk through all files and upload them in parallel using a thread pool
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    source_path = Path(source_folder)
    all_files   = [f for f in source_path.rglob("*") if f.is_file()]

    if not all_files:
        logger.warning(f"No files found in {source_folder}")
        return

    logger.info(f"Found {len(all_files)} file(s) in {source_folder}")
    logger.info(f"Uploading to gs://{bucket_name}/{destination_prefix}/ with {MAX_WORKERS} parallel threads")
    logger.info("-" * 60)

    uploaded, skipped, failed = 0, 0, 0

    # Submit all files to the thread pool and process results as they complete
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for local_path in all_files:
            relative_path = local_path.relative_to(source_path)
            blob_name     = f"{destination_prefix}/{relative_path}".replace("\\", "/")
            future        = executor.submit(upload_file, bucket, local_path, blob_name)
            futures[future] = local_path

        for future in as_completed(futures):
            result = future.result()
            if result == "uploaded":
                uploaded += 1
            elif result == "skipped":
                skipped += 1
            else:
                failed += 1

    logger.info("-" * 60)
    logger.info(f"Upload complete.")
    logger.info(f"  Total files : {len(all_files)}")
    logger.info(f"  Uploaded    : {uploaded}")
    logger.info(f"  Skipped     : {skipped}")
    logger.info(f"  Failed      : {failed}")

if __name__ == "__main__":
    upload_folder(SOURCE_FOLDER, BUCKET_NAME, DESTINATION_PREFIX)