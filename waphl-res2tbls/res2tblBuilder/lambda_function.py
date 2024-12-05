"""
Creates table summaries of the WAPHL-Results SQL database using Athena.
"""
import boto3
import time
import json

#----- CREATE THE SQL DATABASE (AWS GLUE CRAWLER) -----#
# Function to start a Glue crawler and wait for it to finish
def run_crawler(client,crawler_name):
    # Start the crawler
    client.start_crawler(Name=crawler_name)
    print(f"Crawler '{crawler_name}' started.")
    
    # Wait for the crawler to finish
    while True:
        response = client.get_crawler(Name=crawler_name)
        crawler_state = response['Crawler']['State']
        print(f"Crawler '{crawler_name}' state: {crawler_state}")
        
        if crawler_state == 'READY':
            print(f"Crawler '{crawler_name}' has finished.")
            break
        else:
            time.sleep(10)  # Wait for 10 seconds before checking the state again

#----- TABLES REQUIRING AWS ATHENA -----#
# Function for creating tables via Athena
def create_table_athena(athena, s3, database, tmpdir, bucket, key):
    # Define data processing steps
    procc = {
        's1': {'database': database,
               'outdir': tmpdir,
               'queries': [ 
                   """
                   DROP TABLE IF EXISTS meta_alt;
                   """,
                   """
                   CREATE TABLE meta_alt AS
                   SELECT DISTINCT
                   id,
                   CASE
                       WHEN LOWER(workflow) LIKE '%phoenix%' THEN 'phoenix'
                       WHEN LOWER(workflow) LIKE '%theiaprok%' THEN 'theiaprok'
                       WHEN UPPER(workflow) LIKE '%RECAPP%' THEN 'recapp'
                       WHEN UPPER(workflow) LIKE '%BASESPACE%' THEN 'basespace_fetch'
                       ELSE workflow
                   END AS workflow,
                   run,
                   file,
                   timestamp,
                   origin,
                   current,
                   REGEXP_REPLACE(id, '-WA.*', '') AS id_alt
                   FROM meta;
                   """] },
        's2': {'database': database,
               'outdir': tmpdir,
               'queries': ["SELECT * FROM meta;"] },
        's3': {'database': database,
               'outdir': tmpdir,
               'queries': ["SELECT * FROM meta_alt;"] },
        's4': {'database': database,
               'outdir': tmpdir,
               'queries': ["""
                    SELECT * FROM meta_alt 
                    WHERE
                        (id = 'null' AND workflow = 'phoenix' AND file = 'Phoenix_Summary.tsv') OR 
                        (id = 'null' AND workflow = 'phoenix' AND file = 'terra_table.tsv' ) OR
                        (id = 'null' AND workflow = 'theiaprok' AND file = 'terra_table.tsv') OR
                        (id = 'null' AND workflow = 'recapp' AND file = 'terra_table.tsv');
                    """] },
        's5': {'database': database,
               'outdir': tmpdir,
               'queries': ["""
                    SELECT * FROM meta_alt 
                    WHERE 
                        file LIKE '%.fastq.gz%' AND
                        (file LIKE '%R1%' OR file LIKE '%R2%');
                    """] },
        's6': {'database': database,
               'outdir': tmpdir,
               'queries': ["""
                    SELECT * FROM meta_alt 
                    WHERE 
                        file LIKE '%.fasta%' 
                        OR file LIKE '%.fa' 
                        OR file LIKE '%.fa.gz' 
                        OR file LIKE '%.fna%';
                    """] }
    }

    # Run processing steps
    ## empty dictionary for capturing metadata for each processing step
    qids = {}
    for step in procc:
        print(f'Starting {step}')
        db      = procc[step]["database"]
        outdir  = procc[step]["outdir"]
        queries = procc[step]["queries"]
        print(f'  Database: {db}\n  Outdir: {outdir}\n  Queries: {len(queries)}')
        
        qcount=0
        meta = {}
        for q in queries:
            q = " ".join(q.split())
            print(f'\n  {q}\n')
            response = run_query(athena, q, db, outdir)
            waitForQuery(athena, response['QueryExecutionId'])
            meta[qcount] = [q, db, outdir, response['QueryExecutionId']]
            qcount = qcount + 1
        qids[step] = meta

    # Rename tables
    rename_table(s3, bucket, f'{key}/tmp/{qids["s2"][0][3]}.csv', f'{key}/meta.raw.csv')
    rename_table(s3, bucket, f'{key}/tmp/{qids["s3"][0][3]}.csv', f'{key}/meta.clean.csv')
    rename_table(s3, bucket, f'{key}/tmp/{qids["s4"][0][3]}.csv', f'{key}/meta.gba.csv')
    rename_table(s3, bucket, f'{key}/tmp/{qids["s5"][0][3]}.csv', f'{key}/meta.fastq.csv')
    rename_table(s3, bucket, f'{key}/tmp/{qids["s6"][0][3]}.csv', f'{key}/meta.fasta.csv')

    # Clean up unwanted files
    delete_directory(s3, bucket, f'{key}/tmp')


# Function to run an Athena query
def run_query(client, query, db, output):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': db},
        ResultConfiguration={'OutputLocation': output},
        WorkGroup='primary'
    )
    return response

# Function to wait for query completion
def waitForQuery(client, query_execution_id):
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return status
        time.sleep(2)

# Function for renaming the Athena table queries
def rename_table(client, bucket, source_key, dest_key):
    print(f'\nRenaming {source_key} to {dest_key}')
    copy_source = { 'Bucket': bucket, 'Key': source_key }
    client.copy(copy_source, bucket, dest_key)
    client.delete_object(Bucket=bucket, Key=source_key)

# Function to delete directory in S3 bucket
def delete_directory(client, bucket_name, prefix):
    # List all objects in the directory (prefix)
    response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    # Check if there are any objects to delete
    if 'Contents' in response:
        for obj in response['Contents']:
            # Print the file being deleted (optional)
            print(f"Deleting {obj['Key']}")
            
            # Delete the object
            client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        
        # If there are more objects to delete, repeat the process
        while response['IsTruncated']:
            response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=response['NextContinuationToken'])
            if 'Contents' in response:
                for obj in response['Contents']:
                    print(f"Deleting {obj['Key']}")
                    client.delete_object(Bucket=bucket_name, Key=obj['Key'])
    
    print(f"All objects under {prefix} have been deleted.")

#-----TABLES REQUIRING AWS BATCH-----#
# Function for submitting AWS Batch job
def submit_batch_job(client, jobname, jobqueue, jobdef, containeroverrides):
    response = client.submit_job(
                jobName=jobname,
                jobQueue=jobqueue,
                jobDefinition=jobdef,
                containerOverrides=" ".join(containeroverrides.split())
            )

# Function for creating tables that require AWS Batch
def create_table_batch(client, jobqueue, jobdef, bucket, key):
    # General bacterial analysis
    ## Define container overrides - double curly brackets needed to escape f-strings
    gba = f"""
    {{ 
    'environment': [
        {{
            'name': 'BUCKET',
            'value': {bucket}
        }},
        {{
            'name': 'KEY',
            'value': {key}
        }}],
    'command': [
        'bash','-c','git clone https://github.com/DOH-JDJ0303/waphl-data.git && bash waphl-data/waphl-res2tbl/gba/aws-batch-script.sh $BUCKET $KEY'
        ]
        }}
        """
    ## Submit job
    submit_batch_job(client, 'gba_table', jobqueue, jobdef, gba)


#-----HANDLER FUNCTION-----#
def handler(event, contxext):
    # Initialize boto3 session and clients
    session = boto3.Session()
    secrets = session.client('secretsmanager')
    athena  = session.client('athena')
    s3      = session.client('s3')
    glue    = session.client('glue')
    batch   = session.client('batch')

    # Get secrets
    ## Define secret name
    secret_name = 'waphl-res2tbl/2024125'
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
    crawler  = secret["crawler"]
    database = secret["database"]
    bucket   = secret["bucket"]
    key      = secret["key"]
    jobqueue = secret["jobqueue"]
    jobdef   = secret["jobdef"]

    # Other variables
    tabledir  = f's3://{bucket}/{key}'
    tmpdir    = f'{tabledir}/tmp'
    
    # Run Glue crawler to update the Athena database
    run_crawler(glue, crawler)

    # Create tables using Athena
    create_table_athena(athena, s3, database, tmpdir, bucket, key)

    # Create tables using AWS Batch
    create_table_batch(batch, jobqueue, jobdef, bucket, key)

handler("blah","blah")
