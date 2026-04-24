import os
from time import sleep
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from dotenv import load_dotenv
from logger import setup_logger

load_dotenv()
logger = setup_logger(__name__)

BUCKET_NAME        = os.getenv("BUCKET_NAME")
SOURCE_FOLDER      = os.getenv("SOURCE_FOLDER")
DESTINATION_PREFIX = os.getenv("DESTINATION_PREFIX", "")
PROJECT_ID         = os.getenv("GOOGLE_CLOUD_PROJECT")
MAX_WORKERS        = 20
MAX_RETRIES        = 3
RETRY_DELAY        = 2


if not all([BUCKET_NAME, SOURCE_FOLDER]):
    raise EnvironmentError("Missing required env variables. Ensure BUCKET_NAME and SOURCE_FOLDER are set in .env")

if not os.path.exists(SOURCE_FOLDER):
    raise FileNotFoundError(f"Source folder not found at: {SOURCE_FOLDER}")


def get_gcs_client():
    # Uses gcloud logged-in account credentials with explicit project ID
    return storage.Client(project=PROJECT_ID)


def iter_files(source_path: Path):
    # Yield files one at a time instead of loading all paths into memory
    for f in source_path.rglob("*"):
        if f.is_file():
            yield f


def upload_file(bucket, local_path: Path, blob_name: str):
    # Attempt to upload a file with retries — returns "uploaded", "skipped", or "failed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            blob = bucket.blob(blob_name)

            if blob.exists():
                logger.warning(f"Skipped (already exists): {blob_name}")
                return "skipped"

            blob.upload_from_filename(str(local_path))
            logger.info(f"Uploaded: {local_path.name} --> gs://{bucket.name}/{blob_name}")
            return "uploaded"

        except Exception as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {local_path.name}: {e}")
            if attempt < MAX_RETRIES:
                sleep(RETRY_DELAY)

    # All retries exhausted
    logger.error(f"All {MAX_RETRIES} attempts failed for {local_path.name}")
    return "failed"


def upload_folder(source_folder: str, bucket_name: str, destination_prefix: str):
    # Walk through all files and upload them in parallel using a thread pool
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    source_path = Path(source_folder)

    # Count total files first for progress tracking
    logger.info("Counting files...")
    total_files = sum(1 for f in source_path.rglob("*") if f.is_file())

    if total_files == 0:
        logger.warning(f"No files found in {source_folder}")
        return

    logger.info(f"Found {total_files} file(s) in {source_folder}")
    logger.info(f"Uploading to gs://{bucket_name}/{destination_prefix or '(root)'}/ with {MAX_WORKERS} parallel threads")
    logger.info("-" * 60)

    uploaded, skipped, failed = 0, 0, 0
    failed_log = open("failed_uploads.txt", "a")

    # Submit files to thread pool using a generator — memory efficient for large folders
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for local_path in iter_files(source_path):
            relative_path_str = str(local_path.relative_to(source_path)).replace("\\", "/")
            blob_name         = f"{destination_prefix}/{relative_path_str}".strip("/") if destination_prefix else relative_path_str
            future            = executor.submit(upload_file, bucket, local_path, blob_name)
            futures[future]   = local_path

        for i, future in enumerate(as_completed(futures), 1):
            local_path = futures[future]
            result     = future.result()

            if result == "uploaded":
                uploaded += 1
            elif result == "skipped":
                skipped += 1
            else:
                failed += 1
                # Save failed file path to failed_uploads.txt for re-run
                failed_log.write(f"{str(local_path)}\n")

            # Log progress every 1000 files
            if i % 1000 == 0:
                logger.info(f"Progress: {i}/{total_files} files processed | Uploaded: {uploaded} | Skipped: {skipped} | Failed: {failed}")

    failed_log.close()

    logger.info("-" * 60)
    logger.info(f"Upload complete.")
    logger.info(f"  Total files : {total_files}")
    logger.info(f"  Uploaded    : {uploaded}")
    logger.info(f"  Skipped     : {skipped}")
    logger.info(f"  Failed      : {failed}")

    if failed > 0:
        logger.warning(f"  {failed} file(s) failed after {MAX_RETRIES} retries. Check failed_uploads.txt to retry them.")


if __name__ == "__main__":
    upload_folder(SOURCE_FOLDER, BUCKET_NAME, DESTINATION_PREFIX)