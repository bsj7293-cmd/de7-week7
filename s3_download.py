import boto3
import csv
import os
from pathlib import Path

BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_PREFIX = "bronze/"
S3_KEY = "bronze/netflix_titles.csv"

PROJECT_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = PROJECT_DIR / "data"
DOWNLOAD_PATH = DOWNLOAD_DIR / "netflix_titles.csv"

def main():
    s3 = boto3.client("s3")

    print("1. List objects in bronze")

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=S3_PREFIX,
    )

    objects = response.get("Contents", [])

    for obj in objects:
        print(f"{obj['Key']} ({obj['Size']} bytes)")

    print("2. Download netflix_titles.csv")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    s3.download_file(
        BUCKET_NAME,
        S3_KEY,
        str(DOWNLOAD_PATH),
    )

    print(f"Downloaded: s3://{BUCKET_NAME}/{S3_KEY}")
    print(f"Saved to: {DOWNLOAD_PATH}")

    print("\n3. Count CSV records")

    with open(DOWNLOAD_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)

        record_count = sum(1 for _ in reader)

    print(f"CSV record count: {record_count}")

if __name__ == "__main__":
    main()