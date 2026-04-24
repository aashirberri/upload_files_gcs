# GCS Bulk File Uploader

- Uploads all files from a specified local folder to a GCS bucket
- Skips files that already exist in the bucket (safe to re-run)
- Parallel uploads using configurable thread pool (default: 10 threads)
- Logs everything to both console and `upload.log`

---

## Prerequisites

- Python 3.8+
- A Google Cloud Platform account
- A GCS bucket created in your GCP project
- A service account with appropriate permissions (see [GCP Setup](#gcp-setup))

---

## GCP Setup

### 1. Create a Service Account

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Select your project from the top dropdown
3. Navigate to **IAM & Admin** → **Service Accounts**
4. Click **+ Create Service Account**
5. Fill in a name (e.g. `gcs-uploader`) and click **Create and Continue**
6. Assign the role **Storage Object Creator** (upload only, no read/delete)
7. Click **Continue** then **Done**

### 2. Download the JSON Key

1. From the Service Accounts list, click on the account you just created
2. Go to the **Keys** tab
3. Click **Add Key** → **Create new key** → select **JSON** → click **Create**
4. A JSON file will auto-download — place it in your project folder
5. Rename it to `service-account-key.json` or note the path for your `.env`

### 3. Grant Bucket-Level Permission

1. Go to **Cloud Storage** → **Buckets** → click your bucket
2. Go to the **Permissions** tab
3. Click **+ Grant Access**
4. Add your service account email and assign the role **Storage Object Creator**
5. Click **Save**

### 4. Verify Bucket is Private

1. In the **Permissions** tab of your bucket
2. Make sure `allUsers` and `allAuthenticatedUsers` are **not** listed
3. If they are, remove them immediately — your bucket should be private

---

## Local Setup

### 1. Clone the repository

```bash
git clone https://github.com/aashirberri/upload_files_gcs.git
cd upload_files_gcs
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv

# Mac/Linux
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create your `.env` file

Create a `.env` file in the project root with the following values:

```env
SERVICE_ACCOUNT_KEY=./service_account_key.json
BUCKET_NAME=your-bucket-name
SOURCE_FOLDER=/absolute/path/to/your/folder
DESTINATION_PREFIX=candidate-profiles
```

| Variable | Description |
|---|---|
| `SERVICE_ACCOUNT_KEY` | Path to your GCP service account JSON key file |
| `BUCKET_NAME` | Name of your GCS bucket |
| `SOURCE_FOLDER` | Absolute path to the local folder you want to upload |
| `DESTINATION_PREFIX` | Folder name inside the bucket where files will be placed |

> **Note:** `DESTINATION_PREFIX` is optional and defaults to `candidate-profiles` if not set.

---

## Usage

Run the script with:

```bash
python main.py
```

### Example Output

```
2026-04-24 16:52:09 INFO Found 3 file(s) in /home/user/profiles
2026-04-24 16:52:09 INFO Uploading to gs://my-bucket/candidate-profiles/ with 10 parallel threads
2026-04-24 16:52:09 INFO ------------------------------------------------------------
2026-04-24 16:52:10 INFO Uploaded: Aashir_Resume.pdf --> gs://my-bucket/candidate-profiles/Aashir_Resume.pdf
2026-04-24 16:52:10 INFO Uploaded: John_Resume.pdf --> gs://my-bucket/candidate-profiles/John_Resume.pdf
2026-04-24 16:52:11 WARNING Skipped (already exists): candidate-profiles/Jane_Resume.pdf
2026-04-24 16:52:11 INFO ------------------------------------------------------------
2026-04-24 16:52:11 INFO Upload complete.
2026-04-24 16:52:11 INFO   Total files : 3
2026-04-24 16:52:11 INFO   Uploaded    : 2
2026-04-24 16:52:11 INFO   Skipped     : 1
2026-04-24 16:52:11 INFO   Failed      : 0
```

---

## Configuration

You can tune the number of parallel threads by changing `MAX_WORKERS` at the top of `main.py`:

```python
MAX_WORKERS = 10  # default
```

| File type / size | Recommended `MAX_WORKERS` |
|---|---|
| Small files (PDFs, DOCX under 1MB) | 10 - 20 |
| Large files (videos, large archives) | 4 - 5 |
| Not sure | 10 (safe default) |

---

## Logs

Every run appends to `upload.log` in the project root. This gives you a full record of what was uploaded, skipped, or failed across all runs.
