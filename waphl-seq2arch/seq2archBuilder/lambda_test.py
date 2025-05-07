import os
import pandas as pd
import boto3
import json


"""
Maintained profiles for common dataflows in seqera cloud workspaces, workflows,
and buckets
"""

session = boto3.Session()
secrets = session.client('secretsmanager')

def read_config() -> pd.DataFrame:
    secret_name = 'waphl-seq2arch/20250416'
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
    config = pd.DataFrame(list(secret.items()), columns=["name", "info"])
    #config = pd.read_csv('config.csv')
    config.set_index("name", inplace=True)

    return config

conf = read_config()
print(conf)