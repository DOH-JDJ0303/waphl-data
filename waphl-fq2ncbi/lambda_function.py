"""
Creates table summaries of the WAPHL-Results SQL database using Athena.
"""
import boto3
import time
import csv
import json

#-----HANDLER FUNCTION-----#
def handler(event, contxext):
    # Initialize boto3 session and clients
    session = boto3.Session()
    secrets = session.client('secretsmanager')
    s3      = session.client('s3')

    # Get secrets
    ## Define secret name
    secret_name = 'waphl-fq2ncbi/20250107'
    try:
        get_secret_value_response = secrets.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    
    ## Parse secret
    secret = json.loads(get_secret_value_response['SecretString'])
    sourceBucket = secret["sourceBucket"]
    destBucket   = secret["destBucket"]

    # Other variables
    metaKey  = f'tables/fastq.csv'

    # New FASTQ files
    timelimit = int(time.time()) - 30*24*60*60 # first number is days
    response = s3.get_object(Bucket=sourceBucket, Key=metaKey)
    content = list(csv.reader(response['Body'].read().decode('utf-8').splitlines()))
    newFiles = []
    for row in content:
        try:
            if ('_R1' in row[3] or 'R2' in row[3]) and int(row[4]) > timelimit:
                newFiles.append([ f'{row[0]}_{"R1" if "R1" in row[3] else "R2" }.fastq.gz', row[6] ])
        except:
            print(f'WARNING: Skipping {row}')
    
    # Files in desintaion bucket
    response = s3.list_objects_v2(Bucket=destBucket)
    destFiles = [obj['Key'] for obj in response.get('Contents', [])]

    # noTransferList = [row[0] for row in newFiles if row[0] in destFiles]
    # print(f'These files have already been transferred: {"".join(noTransferList)}')
    transferList = [row for row in newFiles if row[0] not in destFiles]
    print(f'Transfering these files: {" ".join(transferList)}')
    for row in transferList:
        copy_source = {'Bucket': sourceBucket, 'Key': row[1].replace(f's3://{sourceBucket}/', '')}
        s3.copy_object(CopySource=copy_source, Bucket=destBucket, Key=row[0])

# handler("blah","blah")
