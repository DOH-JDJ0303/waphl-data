import os
import pandas as pd
import boto3
import json
from botocore.exceptions import ClientError


"""
Maintained profiles for common dataflows in seqera cloud workspaces, workflows,
and buckets
"""

session = boto3.Session()
secrets = session.client('secretsmanager', region_name='us-west-2')

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



def get_s3_buckets():
    dev_s3 = conf.loc["dev_s3", 'info']
    prod_s3 = conf.loc["prod_s3", 'info']
    return prod_s3, dev_s3

def get_tower_access_token() -> str:
    tower_access_token = conf.loc["tower_access_token", 'info']
    return tower_access_token


def bac_values() -> list:
    bs_api = conf.loc["bs_micro_api", 'info']
    #s3 = conf.loc["prod_s3", 'info']
    next_pipe = "phoenix"
    compute_env = conf.loc["waphl_bac_compute", 'info']
    workspace_id = conf.loc["waphl_bac_id", 'info']
    tableset = 'bacteria'
    tag = ['bacteria']
    return bs_api, next_pipe, compute_env, workspace_id, tableset, tag


def hai_values() -> list:
    bs_api = conf.loc["bs_hai_api", 'info']
    next_pipe = "phoenix"
    compute_env = conf.loc["waphl_bac_compute", 'info']
    workspace_id = conf.loc["waphl_bac_id", 'info']
    tableset = 'bacteria'
    tag = ['bacteria', 'hai']
    return bs_api, next_pipe, compute_env, workspace_id, tableset, tag


def vir_values() -> list:
    bs_api = conf.loc["bs_micro_api", 'info']
    #s3 = conf.loc["prod_s3", 'info']
    next_pipe = ""
    compute_env = conf.loc["waphl_viral_compute", 'info']
    workspace_id = conf.loc["waphl_viral_id", 'info']
    tableset = 'virus'
    tag = ['virus']
    return bs_api, next_pipe, compute_env, workspace_id, tableset, tag


def fungi_values() -> list:
    bs_api = conf.loc["bs_micro_api", 'info']
    #s3 = conf.loc["prod_s3", 'info']
    next_pipe = ""
    compute_env = conf.loc["waphl_fungi_compute", 'info']
    workspace_id = conf.loc["waphl_fungi_id", 'info']
    tableset = 'fungi'
    tag = ['fungi']
    return bs_api, next_pipe, compute_env, workspace_id, tableset, tag


def mycosnp_values() -> list:
    bs_api = conf.loc["bs_micro_api", 'info']
    #s3 = conf.loc["prod_s3", 'info']
    next_pipe = ""
    compute_env = conf.loc["MycoSNP_1_compute", 'info']
    workspace_id = conf.loc["MycoSNP_1_id", 'info']
    tableset = 'fungi'
    tag = ['fungi', "mycosnp"]
    return bs_api, next_pipe, compute_env, workspace_id, tableset, tag


def dev_values(project: str) -> list:
    """
    Retrieve and set development values based on the project type. 
    This function retrieves the S3 bucket information from the configuration 
    and determines the appropriate BaseSpace API key, next pipeline, compute 
    environment, and workspace ID based on the provided project type. It then 
    overwrites the compute environment and workspace ID with the development 
    values and returns the final set of values. 
    Args: 
        project (str): The name of the project, such as 'bacteria', 
                'shigella_pilot', 'viral', or 'hai'. 
    Returns: list: A list containing the BaseSpace API key, S3 bucket, 
                next pipeline, compute environment, workspace ID, relevant 
                tracker tableset, and aws tags.
    """
    s3 = conf.loc["dev_s3", 'info']
    # take bs_api and nextpipe from project values
    if project in ["bacteria", "shigella_pilot"]:
        bs_api, next_pipe, compute_env, workspace_id, tableset, tag = bac_values()
    elif project in ["virus"]:
        bs_api, next_pipe, compute_env, workspace_id, tableset, tag = vir_values()
    elif project in ["hai"]:
        bs_api, next_pipe, compute_env, workspace_id, tableset, tag= hai_values()
    elif project in ["mycosnp"]:
        bs_api, next_pipe, compute_env, workspace_id, tableset, tag = mycosnp_values()
    elif project in ["fungi"]:
        bs_api, next_pipe, compute_env, workspace_id, tableset, tag = fungi_values()
    # Add dev tags and create tag string
    tag.append("dev")
    # Overwrite compute and workspace values to be dev
    compute_env = conf.loc["waphl_dev_compute", 'info']
    workspace_id = conf.loc["dev_id", 'info']

    return [bs_api, s3, next_pipe, compute_env, workspace_id, tableset, tag]


def prod_values(project: str) -> list:
    """
    Retrieve and set production values based on the project type. This function
    retrieves the S3 bucket information from the configuration and determines 
    the appropriate BaseSpace API key, next pipeline, compute environment, and 
    workspace ID based on the provided project type. It then returns the final 
    set of values for the production environment. 
    Args: 
        project (str): The name of the project, such as 'bacteria', 
                'shigella_pilot', 'viral', or 'hai'. 
    Returns: 
        list: A list containing the BaseSpace API key, S3 bucket, 
                next pipeline, compute environment, , workspace ID, relevant 
                tracker tableset, and aws tags
    """
    s3 = conf.loc["prod_s3", 'info']
    if project in ["bacteria", "shigella_pilot"]:
        bs_api, next_pipe, compute_env, workspace_id, tableset, tag = bac_values()
    elif project in ["virus"]:
        bs_api, next_pipe, compute_env, workspace_id, tableset, tag = vir_values()
    elif project in ["hai"]:
        bs_api, next_pipe, compute_env, workspace_id, tableset, tag = hai_values()
    elif project in ["mycosnp"]:
        bs_api, next_pipe, compute_env, workspace_id, tableset, tag = mycosnp_values()
    elif project in ["fungi"]:
        bs_api, next_pipe, compute_env, workspace_id, tableset, tag = fungi_values()
    # Add production tags and create tag string
    tag.append("Production")
    return [bs_api, s3, next_pipe, compute_env, workspace_id, tableset, tag]