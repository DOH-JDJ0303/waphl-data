import os
import boto3

# localize google credentials, if needed
google_credentials = "s3://waphl-results/assets/.config/gcloud/application_default_credentials.json"
s3_client    = boto3.client('s3')
gcred_split  = google_credentials.replace('s3://','').split('/')
gcred_bucket = gcred_split[0]
gcred_key    = '/'.join(gcred_split[1:])
gcred_local  = "google_credentials.json"
s3_client.download_file(gcred_bucket, gcred_key, gcred_local)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcred_local