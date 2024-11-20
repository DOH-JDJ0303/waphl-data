from firecloud import api as fapi
import argparse
import json
from datetime import datetime
import re
import boto3

if __name__ == "__main__":
    # argument parser
    parser = argparse.ArgumentParser(description="Exports/downloadload a TSV file from Terra when it is too large to download via the UI.")
    # application arguments
    parser.add_argument('-p', '--project', type=str, required=True, help='Terra namespace/project of workspace.')
    parser.add_argument('-w', '--workspace', type=str, required=True, help='Name of Terra workspace.')
    parser.add_argument('-o', '--outdir', type=str, required=True, help='URI to output directory.')
    args = parser.parse_args()

    # Get the current date and time 
    now = int(datetime.now().timestamp())

    r = fapi.list_submissions(args.project, args.workspace)
    fapi._check_response_code(r, 200)
    runs = {}
    for elem in r.json():
        setst    = bool(re.search(r'_set',elem["submissionEntity"]["entityType"]))
        runst    = elem["status"] == "Done"
        samplest = "Succeeded" in elem["workflowStatuses"]
        if not setst and runst and samplest:
            runs[elem["submissionId"]] = elem["submissionEntity"]["entityType"]
    
    s3_bucket = args.outdir.replace('s3://', '').replace('/','')
    cache_key = f'cache/terra/{args.project}_{args.workspace}.csv'

    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=cache_key) 
        cache = response['Body'].read().decode('utf-8').split('\n')
        newids = [id for id in list(runs.keys()) if id not in cache]
        newruns = {key: runs[key] for key in newids if key in runs}
    except:
        newruns = runs

    # send SQS message for each new run
    # Create an SQS client 
    sqs = boto3.client('sqs')
    # URL of the SQS queue 
    queue_url = 'insert url here'  
    for run in newruns:
        # Message to send
        msg =  f'{{"project":"{args.project}", "workspace":"{args.workspace}", "run":"{run}"}}'
        # Send the message
        response = sqs.send_message( QueueUrl=queue_url, MessageBody=msg ) 
        # Print out the response 
        print(response)


