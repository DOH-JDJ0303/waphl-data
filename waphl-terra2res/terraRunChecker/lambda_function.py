# determines new Terra runs using the submission ID and submits an SQS message
# checks submission ID cache if it is available, otherwise all runs are submitted

from firecloud import api as fapi
import json
from datetime import datetime
import re
import boto3
from botocore.exceptions import ClientError
import os

def terraRunChecker(project,workspace,bucket,queue_url):
    # create dictionary of Terra entities (runs) and their submission IDs
    ## get list of Terra submissions
    r = fapi.list_submissions(project, workspace)
    fapi._check_response_code(r, 200)
    runs = {}
    for elem in r.json():
        setst    = bool(re.search(r'_set',elem["submissionEntity"]["entityType"]))
        runst    = elem["status"] == "Done"
        samplest = "Succeeded" in elem["workflowStatuses"]
        if not setst and runst and samplest:
            runs[elem["submissionId"]] = elem["submissionEntity"]["entityType"]

    # determine if there any new runs based on the submission IDs and existing ID cache
    cache_key = f'cache/terra/{project}/{workspace}/'
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2( Bucket=bucket, Prefix=cache_key, Delimiter='/' )
    if 'Contents' in response: 
        cache = []
        for obj in response['Contents']:
            cache.append(obj['Key'].replace(cache_key, ''))
        newids = [id for id in list(runs.keys()) if id not in cache]
        newruns = {key: runs[key] for key in newids if key in runs}
    else:
        newruns = runs
    
    # send SQS message for each new run
    # Create an SQS client 
    sqs = boto3.client('sqs')
    #URL of the SQS queue  
    for run in newruns:
        # Message to send
        msg =  f'{{"project":"{project}", "workspace":"{workspace}", "submissionId":"{run}", "submissionEntity":"{newruns[run]}"}}'
        print(msg)
        # Send the message
        response = sqs.send_message( QueueUrl=queue_url, MessageBody=msg ) 
        # Print out the response 
        print(response)

def handler(event, context):
    # get secrets
    secret_name = "terraRunChecker/1"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    
    # parse secret
    secret = json.loads(get_secret_value_response['SecretString'])
    terra_project      = secret["terra_project"]
    terra_workspaces   = secret["terra_workspaces"].split(',')
    aws_results_bucket = secret["aws_results_bucket"]
    aws_prod_sqs_url   = secret["aws_prod_sqs_url"]
    google_credentials = secret["google_credentials"]

    # set gcloud credentials
    google_credentials = "s3://waphl-results/assets/.config/gcloud/application_default_credentials.json"
    s3_client    = boto3.client('s3')
    gcred_split  = google_credentials.replace('s3://','').split('/')
    gcred_bucket = gcred_split[0]
    gcred_key    = '/'.join(gcred_split[1:])
    gcred_local  = "google_credentials.json"
    s3_client.download_file(gcred_bucket, gcred_key, gcred_local)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcred_local

    # iterate over workspaces
    for wksp in terra_workspaces:
        terraRunChecker(terra_project, wksp, aws_results_bucket, aws_prod_sqs_url)

# # for dev
# handler('test','test')

