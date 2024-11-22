"""
Submits an AWS Batch job to run waphl-prod2res for each run supplied in a list
"""
import json
from datetime import datetime
import re
import boto3
from botocore.exceptions import ClientError
import os
import argparse

def submit_job(workflow, run, outdir, jobqueue, jobdef):
    cmd = """
    git clone https://github.com/DOH-JDJ0303/waphl-data.git && \
echo "workflow,run" > samplesheet.csv && \
echo "$WORKFLOW,$RUN" >> samplesheet.csv && \
nextflow run waphl-data/waphl-prod2res/main.nf --input samplesheet.csv --retention_schema waphl-data/waphl-prod2res/retention-schemes.config --outdir $OUTDIR; \
PREFIX=$(cat .nextflow.log | grep "Files will be saved with prefix:" | cut -f 4 -d ':' | tr -d ' ') && aws s3 cp .nextflow.log ${OUTDIR%%/}/logs/${PREFIX}-waphl-prod2res.log
""".strip()
    
    batch_client = boto3.client('batch')
    response = batch_client.submit_job(
            jobName=f'{workflow}_{run.split("/")[-2]}',
            jobQueue=jobqueue,
            jobDefinition=jobdef,
            containerOverrides={
                'environment': [
                    {
                        'name': 'WORKFLOW',
                        'value': workflow
                    },
                    {
                        'name': 'RUN',
                        'value': run
                    },
                    {
                        'name': 'OUTDIR',
                        'value': outdir
                    }
                ],
                'command': ['bash','-c',cmd]
            }
        )
    print(response)

def check_file_exists(bucket_name, file_key):
    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except ClientError as e:
        # Check if the exception is for a missing object
        if e.response['Error']['Code'] == '404':
            return False
        else:
            # If there is another error, re-raise it
            raise
    
if __name__ == "__main__":
    # argument parser
    parser = argparse.ArgumentParser(description="Exports/downloadload a TSV file from Terra when it is too large to download via the UI.")
    # application arguments
    parser.add_argument('-i', '--input', type=str, required=True, help='Path to file containing workflow names and run directory URIs - e.g., phoenix,s3://bucket/run/')
    parser.add_argument('-o', '--outdir', type=str, required=True, help='Output URI')
    parser.add_argument('-q', '--job_queue', type=str, required=True, help='AWS Batch Job Queue')
    parser.add_argument('-d', '--job_definition', type=str, required=True, help='AWS Batch Job Definition')

    args = parser.parse_args()

    f = open(args.input, "r")
    runs = []
    for line in f:
        linestripped = line.strip().split(',')
        workflow = linestripped[0]
        run = linestripped[1]
        runs.append([workflow, run])
    
    # check that each run URI exists
    for line in runs:
        bucket = line[1].replace('s3://','').split('/')[0]
        key = line[1].replace(f's3://{bucket}/', '')
        if not check_file_exists(bucket, key):
            raise ValueError(f'{line[1]} does not exist!')

    # submit batch job for each run
    for line in runs:
        submit_job(line[0],line[1],args.outdir,args.job_queue,args.job_definition)