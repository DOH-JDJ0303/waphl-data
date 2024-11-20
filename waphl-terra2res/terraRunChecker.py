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
    parser.add_argument('-d', '--delta', type=int, required=True, help='Timespan to consider a run as new')
    args = parser.parse_args()

    # Get the current date and time 
    now = int(datetime.now().timestamp())
    timespan = now - args.delta

    r = fapi.list_submissions(args.project, args.workspace)
    fapi._check_response_code(r, 200)
    newruns = []
    for elem in r.json():
        timest   = int(datetime.strptime(elem["submissionDate"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()) > timespan
        setst    = bool(re.search(r'_set',elem["submissionEntity"]["entityType"]))
        runst    = elem["status"] == "Done"
        samplest = "Succeeded" in elem["workflowStatuses"]
        if timest and not setst and runst and samplest:
            newruns.append(elem["submissionEntity"]["entityType"])
    # get unique values
    newruns = list(set(newruns))

    # send SQS message for each new run
    # Create an SQS client 
    sqs = boto3.client('sqs')
    # URL of the SQS queue 
    queue_url = 'https://sqs.us-west-2.amazonaws.com/398869308272/waphl-production-queue'  
    for run in newruns:
        # Message to send 
        msg =  f'{{"project":"{args.project}", "workspace":"{args.workspace}", "run":"{run}"}}'
        # Send the message 
        response = sqs.send_message( QueueUrl=queue_url, MessageBody=msg ) 
        # Print out the response 
        print(response)


