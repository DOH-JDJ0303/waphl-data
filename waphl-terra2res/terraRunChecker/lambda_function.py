"""
Determines new Terra runs using the submission ID and submits an AWS Batch job for each.
Checks submission ID cache if it is available, otherwise all runs are submitted.
Cache is updated at the end of batch job
"""
from firecloud import api as fapi
import json
from datetime import datetime
import re
import boto3
from botocore.exceptions import ClientError
import os

def terraRunChecker(project,workspace,bucket,jobqueue,jobdef,gcred):
    # create dictionary of Terra entities (runs) and their submission IDs
    # get list of Terra entities (tables)
    t = fapi.list_entity_types(project, workspace)
    if t.status_code != 200:
        print(t.text)
        exit(1)
    entity_types_json = t.json()
    ## get list of Terra submissions
    r = fapi.list_submissions(project, workspace)
    fapi._check_response_code(r, 200)
    runs = {}
    for elem in r.json():
        entityst = elem["submissionEntity"]["entityType"] in entity_types_json # check that the entity has data available
        runst    = elem["status"] == "Done" # check the the entity is done
        samplest = "Succeeded" in elem["workflowStatuses"] # check that the entity has samples that succeeded
        if entityst and runst and samplest:
            runs[elem["submissionId"]] = [ elem["methodConfigurationName"], elem["submissionEntity"]["entityType"], elem["submissionDate"] ]
    
    # select most recent version of each entity
    most_recent = {} 
    for key, value in runs.items(): 
        name = value[1] 
        current_time = datetime.strptime(value[2], '%Y-%m-%dT%H:%M:%S.%fZ') 
        # If the name is not in most_recent or the current time is more recent, update the entry 
        if name not in most_recent or current_time > datetime.strptime(most_recent[name][2], '%Y-%m-%dT%H:%M:%S.%fZ'): 
            most_recent[name] = value 
    runs = {k: v for k, v in runs.items() if v[1] in most_recent and v == most_recent[v[1]]}

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

    # limit to 20 runs 
    if len(newruns) > 10:
        newruns = {k: newruns[k] for k in list(newruns)[:19]}
    
    # submit a batch job for each new run
    batch_client = boto3.client('batch')
    for run in newruns:
        response = batch_client.submit_job(
                jobName=f'{project}_{workspace}_{run}',
                jobQueue=jobqueue,
                jobDefinition=jobdef,
                containerOverrides={
                    'environment': [
                        {
                            'name': 'TERRA_PROJECT',
                            'value': project
                        },
                        {
                            'name': 'TERRA_WORKSPACE',
                            'value': workspace
                        },
                        {
                            'name': 'TERRA_SUBMISSIONID',
                            'value': run
                        },
                        {
                            'name': 'TERRA_WORKFLOW',
                            'value': newruns[run][0]
                        },
                        {
                            'name': 'TERRA_SUBMISSIONENTITY',
                            'value': newruns[run][1]
                        },
                        {
                            'name': 'S3_OUTDIR',
                            'value': bucket
                        },
                        {
                            'name': 'GCRED',
                            'value': gcred
                        },
                    ],
                    'command': ['bash','-c','git clone https://github.com/DOH-JDJ0303/waphl-data.git && bash waphl-data/waphl-terra2res/aws-batch-script.sh $TERRA_PROJECT $TERRA_WORKSPACE $TERRA_SUBMISSIONID $TERRA_WORKFLOW $TERRA_SUBMISSIONENTITY $S3_OUTDIR $GCRED']
                }
            )

def handler(event, context):
    # get secrets
    secret_name = 'waphl-terra2res/241121'
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
    aws_job_queue      = secret["aws_job_queue"]
    aws_job_def        = secret["aws_job_def"]
    google_credentials = secret["google_credentials"]

    # set gcloud credentials
    s3_client    = boto3.client('s3')
    gcred_split  = google_credentials.replace('s3://','').split('/')
    gcred_bucket = gcred_split[0]
    gcred_key    = '/'.join(gcred_split[1:])
    gcred_local  = "/tmp/google_credentials.json"
    s3_client.download_file(gcred_bucket, gcred_key, gcred_local)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcred_local

    # iterate over workspaces
    for wksp in terra_workspaces:
        terraRunChecker(terra_project, wksp, aws_results_bucket, aws_job_queue, aws_job_def, google_credentials)

# # for dev
# handler('test','test')

