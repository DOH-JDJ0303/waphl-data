"""
Creates archive binfx runs on seqera to s3.
"""
import boto3
import time
import json
import subprocess
import os
from botocore.exceptions import ClientError
import pandas as pd


# s3_client = boto3.client('s3')

# session = boto3.Session()
# secrets = session.client('secretsmanager', region_name='us-west-2')

def run_cleaning_script():
    """
    Execute the SeqeraKit command with the specified launch configuration. This
    function defines and executes a SeqeraKit command using the provided launch
    configuration. It captures and prints the output of the command. If the 
    command encounters an error, it prints the error details. 
    Args: 
        launch (str): The launch configuration for the SeqeraKit command.
    Raises: 
        subprocess.CalledProcessError: If the SeqeraKit command fails.
    """
    # Define the SeqeraKit command

    tw_command = ["bash", "run_clean_seqera.sh"]
    print(tw_command)
    try:
        # Run the SeqeraKit command
        result = subprocess.run(tw_command, check=True, text=True, 
                                capture_output=True)

        # Print the output of the command
        print("Command Output:")
        print(result.stdout + result.stderr)
        #print("/n/n/n lets test for output")
        #print(f"Output: {result.stdout}")
        return result.stdout

    except subprocess.CalledProcessError as e:
        print("Error running SeqeraKit command:")
        print("Error running SeqeraKit command:") 
        print(f"Return Code: {e.returncode}") 
        print(f"Command: {e.cmd}") 
        print(f"Output: {e.output}") 
        print(f"Error: {e.stderr}")

# def read_config() -> pd.DataFrame:
#     secret_name = 'waphl-seq2arch/20250416'
#     try:
#         get_secret_value_response = secrets.get_secret_value(
#             SecretId=secret_name
#         )
#     except ClientError as e:
#         # For a list of exceptions thrown, see
#         # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
#         raise e
    
#     ## Parse secret
#     secret = json.loads(get_secret_value_response['SecretString'])
#     config = pd.DataFrame(list(secret.items()), columns=["name", "info"])
#     #config = pd.read_csv('config.csv')
#     config.set_index("name", inplace=True)

#     return config
#-----HANDLER FUNCTION-----#
def handler(event, context):
    output = run_cleaning_script()
    return output
  

#handler("blah","blah")
