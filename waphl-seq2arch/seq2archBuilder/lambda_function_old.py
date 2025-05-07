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

from profiles import dev_values, get_tower_access_token



def check_object_exists(bucket,key) -> bool:
    """
    Check if an object exists in an S3 bucket. This function takes an S3 object
    as input, determines its bucket and key, and checks if the object exists in
    the specified S3 bucket. If the object exists, it returns True. If the 
    object does not exist, it returns False. 
    Args: 
        s3_object (object): The S3 object to check. 
    Returns: 
        bool: True if the object exists, False otherwise.
    """

    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        print(f"Object '{key}' exists in bucket '{bucket}'.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Object '{key}' does not exist in bucket '{bucket}'.")
        else:
            print(f"Error: {e}")
        return False


def open_tar(runid):
    tar_command = ["tar", "-xvzf", f"/tmp/{runid}.tar.gz", "-C", "/tmp"] 
    try:
    # Run the SeqeraKit command
        result = subprocess.run(tar_command, check=True, text=True, 
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


tower_access_token = get_tower_access_token()
test=dev_values('bacteria')
s3_bucket = test[1] #bs_api, s3, next_pipe, compute_env, workspace_id, tableset, tag]
workspace_id = test[4]
runid= "J81IjWdudqrsD"
s3_client = boto3.client('s3')

session = boto3.Session()
secrets = session.client('secretsmanager', region_name='us-west-2')

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
#-----HANDLER FUNCTION-----#
def handler(event, context):
    output = run_cleaning_script()
    return output
  

    # #tw_command = ["tw", "-h"]
    # #tw_command = ["bash", "run_clean_seqera_dryrun_short.sh"]
    # tw_command = ["tw", f"-t {tower_access_token}", "runs", "dump", "--workspace", f"{workspace_id}", "-i", f"{runid}", f"--output=/tmp/{runid}.tar.gz"]
    # print(tw_command)
    # try:
    #     # Run the SeqeraKit command
    #     result = subprocess.run(tw_command, check=True, text=True, 
    #                             capture_output=True)

    #     # Print the output of the command
    #     print("Command Output:")
    #     print(result.stdout + result.stderr)
    #     #print("/n/n/n lets test for output")
    #     #print(f"Output: {result.stdout}")
    #     #return result.stdout

    # except subprocess.CalledProcessError as e:
    #     print("Error running SeqeraKit command:")
    #     print("Error running SeqeraKit command:") 
    #     print(f"Return Code: {e.returncode}") 
    #     print(f"Command: {e.cmd}") 
    #     print(f"Output: {e.output}") 
    #     print(f"Error: {e.stderr}")
        
    # #print(output)

    # #     return "test", e.stderr, e.output, e.returncode
    # current_dir_contents = os.listdir('.')
    # print(current_dir_contents)
    # open_tar(runid)
    # s3_client.upload_file("/tmp/workflow.json", "waphl-holly-hallstead-bucket" , f"transfer_test/{runid}.workflow.json")
    # os.remove(f"/tmp/{runid}.tar.gz")
    # check = check_object_exists("waphl-holly-hallstead-bucket" , f"transfer_test/{runid}.workflow.json")
    # return current_dir_contents, result.stdout,check
#handler("blah","blah")
