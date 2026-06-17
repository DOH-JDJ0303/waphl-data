import os
import boto3

from shared.aws_ops import log_print, parse_uri

GOOGLE_CLOUD_CREDENTIALS = os.environ.get('GOOGLE_CLOUD_CREDENTIALS')

def set_google_credentials():
    try:
        session = boto3.session.Session()
        s3 = session.client("s3")
        bucket, key = parse_uri(GOOGLE_CLOUD_CREDENTIALS)
        gcred_local = '/tmp/application_default_credentials.json'
        
        log_print(f"GOOGLE_CLOUD_CREDENTIALS: {GOOGLE_CLOUD_CREDENTIALS}")
        log_print(f"Bucket: {bucket}, Key: {key}")
        log_print(f"Downloading to: {gcred_local}")
        log_print(f"CWD: {os.getcwd()}")
        
        if os.path.exists(gcred_local):
            os.remove(gcred_local)
        
        s3.download_file(bucket, key, gcred_local)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcred_local
        log_print("Google Cloud credentials configured successfully.")
    except Exception as e:
        log_print(f"ERROR: Failed to set Google credentials: {e}")
        raise
