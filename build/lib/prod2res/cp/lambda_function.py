import os
import sys
import json
import csv
import io
import logging
import boto3
import urllib.parse
import concurrent.futures
from botocore.exceptions import ClientError

# ----- Global Setup (run once) ----- #
DEST_BUCKET = os.environ.get('DEST_BUCKET')

session = boto3.session.Session()
s3 = session.client("s3")

# ----- Extract Records from SQS Event ----- #
def log_print(msg):
    print(str(msg), flush=True)

def extract_record(event):
    log_print("Extracting batch event records")
    print("Event record:\n", json.dumps(event, indent=2), flush=True)

    if not event.get("Records"):
        sys.exit("Error: No records found in the event")

    record = event["Records"][0]
    msgid = record.get("messageId")
    if not msgid:
        sys.exit("Error: messageId not found in record")

    try:
        batch = json.loads(record["body"])
    except (KeyError, json.JSONDecodeError) as e:
        sys.exit(f"Error: Failed to parse message body as JSON list - {e}")

    if not isinstance(batch, list) or not batch:
        sys.exit("Error: Message body is not a non-empty list")

    required_fields = {"SOURCE_BUCKET", "SOURCE_KEY", "DEST_BUCKET", "DEST_KEY"}
    for i, item in enumerate(batch):
        if not all(field in item for field in required_fields):
            sys.exit(f"Error: Missing required fields in batch item {i}: {item}")

    log_print(f"Extracted {len(batch)} file copy instructions from message {msgid}")
    return batch, msgid

# ----- S3 Copy Logic -----
def copy_object(record):
    try:
        copy_source = {
            "Bucket": record["SOURCE_BUCKET"],
            "Key": record["SOURCE_KEY"]
        }
        s3.copy_object(
            CopySource=copy_source,
            Bucket=record["DEST_BUCKET"],
            Key=record["DEST_KEY"]
        )
        log_print(f"Copied {record['SOURCE_KEY']} from {record['SOURCE_BUCKET']} to {record['DEST_KEY']} in {record['DEST_BUCKET']}")
        return True, record

    except Exception as e:
        logging.error(f"Failed to copy {record['SOURCE_KEY']} from {record['SOURCE_BUCKET']} to {record['DEST_KEY']} in {record['DEST_BUCKET']}: {e}")
        record["error"] = str(e)
        return False, record

def copy_objects_in_parallel(records, max_workers=5):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(copy_object, rec): rec for rec in records}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return results

# ----- Failure Logging -----
def log_failures(copy_results, filename):
    failed = [r for success, r in copy_results if not success]
    if not failed:
        return

    logging.warning(f"{len(failed)} copy operations failed. Writing error report.")

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["SOURCE_BUCKET", "SOURCE_KEY", "DEST_BUCKET", "DEST_KEY", "error"])
    writer.writeheader()

    for r in failed:
        writer.writerow({
            "SOURCE_BUCKET": r["SOURCE_BUCKET"],
            "SOURCE_KEY": r["SOURCE_KEY"],
            "DEST_BUCKET": r["DEST_BUCKET"],
            "DEST_KEY": r["DEST_KEY"],
            "error": r.get("error", "Unknown")
        })

    s3.put_object(
        Bucket=DEST_BUCKET,
        Key=filename,
        Body=output.getvalue()
    )
    log_print(f"Error report uploaded to s3://{DEST_BUCKET}/{filename}")

# ----- Lambda Handler -----
def handler(event, context):
    records, msgid = extract_record(event)
    copy_results = copy_objects_in_parallel(records, max_workers=3)
    log_failures(copy_results, f"errors/{msgid}.csv")

# ----- Local Test Entry Point -----
if __name__ == "__main__":
    with open("event.json") as f:
        test_event = json.load(f)
    handler(test_event, None)