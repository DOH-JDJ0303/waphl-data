import os
import re
import pandas as pd
import time
import json
from pathlib import Path
import yaml
import subprocess
import glob
import boto3
from botocore.exceptions import ClientError
from argparse import ArgumentParser
from profiles import bac_values, get_s3_buckets, get_tower_access_token, prod_values, dev_values
from datetime import datetime, timezone


allowed_production = ['production', 'dev']
allowed_status = ['FAILED', 'CANCELLED', 'SUCCEEDED']

parser = ArgumentParser()
parser.add_argument("--seq_project", dest="project",  default="shigella_pilot", help="shigella_pilot,bacteria, mycosnp, virus")
parser.add_argument("--production-level", dest="pl",  default="dev", choices=allowed_production, help="production or dev" )
parser.add_argument("--workflow", nargs="+", dest="wf",  default=None, help="workflow name(s)" )
parser.add_argument('--status', nargs="+", dest = "status", default=None, choices=allowed_status,help="remove runs by FAILED, CANCELLED, or SUCCEEDED")
parser.add_argument('--run-name', nargs="+", dest = "rn", default=None, help="one or more run_name")
parser.add_argument('--username', nargs="+", dest = "usr", default=None, help="one or more run_name")
parser.add_argument('--remove-tags', nargs="+", dest = "rmtags", default=None,
                     help="remove runs be tags used in run submission such as bacteria, dev, etc\nif multiple tags provided, will apply function to rows \ncontaining all provided tags" )
parser.add_argument('--keep-days', dest = "days", default=None, type=int, help="number of days to keep" )
parser.add_argument('--dry-run', dest = "dr",  action="store_true", help="shows what runs would be remove, but does not remove them" )
parser.add_argument('--force-archive', dest = "fa",  action="store_true", help="archive dev workspace runs which are not archived by default" )
parser.add_argument('--archive-bucket', dest = "ab",  default="glacier-waphl-seqera", help="s3 bucket where files will get archived" )


args = parser.parse_args() 
project = args.project
pl = args.pl
workflow = args.wf
status = args.status
run_name = args.rn
usr = args.usr
remove_tags = args.rmtags
days = args.days
dryrun = args.dr
archive = args.fa
s3_glacier_bucket = args.ab

# Set the automated version of this to always archive
archive = True


# Set Universal variables
tower_access_token = get_tower_access_token()
norm_df = None
ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
query = "p\nl\na\nc\ne"
page = 1
s3_client = boto3.client('s3')


if pl == "dev":
    li = dev_values(project) 
    workspace_id =  li[4]
    tableset = li[5]
    tag = li[6]

 
elif pl == "production":
    print(project)
    li = prod_values(project) 
    workspace_id =  li[4]
    tableset = li[5]
    tag = li[6] 


def archive_run(run_list, df_columns, pl, workspace_id, tower_access_token):
    """run_list values are ID, Status, Project_Name, Run_Name, Username, Submit_Date, Labels, days"""
    result_dict = dict(zip(df_columns, run_list))

    date = int(convert_to_epoch(result_dict["Submit_Date"]))
    download_run(result_dict["ID"], workspace_id, tower_access_token)
    open_tar(result_dict["ID"])

    for fi in list_files_in_directory("/tmp/"):
            if fi.endswith("json"):
                new_filename = f"{result_dict['Run_Name']}.{result_dict['Project_Name'].split('/')[-1]}.{fi}"
                s3_key = f"productionlevel={pl}/tableset={tableset}/workflow={result_dict['Project_Name'].split('/')[-1]}/run={result_dict['Run_Name']}/filename={new_filename}/status={result_dict['Status']}/timestamp={date}/{new_filename}"
                s3_client.upload_file(f"/tmp/{fi}", s3_glacier_bucket, s3_key, ExtraArgs={'StorageClass': 'DEEP_ARCHIVE'})
                check = check_object_exists(s3_glacier_bucket, s3_key)
                if not check:
                    print(f"Oh no!{s3_key} was not able to be uploaded to {s3_glacier_bucket}")
                    exit(1)
                else:
                    os.remove(f"/tmp/{fi}") 
    os.remove(f"/tmp/{result_dict['ID']}.tar.gz")             
    return check
    

# Function to check if the object exists
def check_object_exists(bucket: str, key: str) -> bool:
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


def convert_to_epoch(date_string: str) -> float:
    """
    Converts a date string into the corresponding epoch time (number of seconds since January 1, 1970).

    Parameters:
    - date_string (str): A date/time string formatted as "Day, DD Mon YYYY HH:MM:SS TZ"
      Example: "Wed, 06 May 2025 13:26:57 UTC"

    Returns:
    - float: The epoch time corresponding to the parsed date.
    """

    parsed_date = datetime.strptime(date_string, "%a, %d %b %Y %H:%M:%S %Z")
    # Convert parsed_date to epoch
    epoch_time = parsed_date.timestamp()
    return epoch_time


# Define the function to calculate days from now
def days_from_now(date_string):
    """
    Calculates the absolute number of days between a provided date and the current date/time.
    
    The function expects the date_string to be formatted as:
      "Day, DD Mon YYYY HH:MM:SS TZ" 
    (e.g., "Wed, 06 May 2025 13:26:57 UTC").
    
    It parses the input string into a datetime object, gets the current UTC time, converts
    it to a timezone-naive datetime (to match the parsed date), and then computes the absolute day difference.
    
    Parameters:
    - date_string (str): The date string to compare against the current date.
    
    Returns:
    - int: The absolute difference in days between the parsed date and the current date.
    - None: If the date_string does not match the expected format.
    """
    try:
        # Parse the date string
        parsed_date = datetime.strptime(date_string, "%a, %d %b %Y %H:%M:%S %Z")
        
        # Get the current date and time
        current_date = datetime.now(timezone.utc)
        # Convert offset-aware to offset-naive
        current_date = current_date.replace(tzinfo=None)
        
        # Calculate the difference in days
        return abs((parsed_date - current_date).days)
    except ValueError:
        return None  # Handle invalid date format


def check_for_tags(lista, listb):
    """
    Checks whether all elements (tags) in 'lista' are present in 'listb'.

    This function converts both input lists into sets and determines if every element 
    from 'lista' is included in 'listb'. A return value of True indicates that 'lista' 
    is a subset of 'listb'; otherwise, it returns False.

    Parameters:
    - lista (list): A list containing the tags to search for.
    - listb (list): A list containing the available tags.

    Returns:
    - bool: True if every tag in 'lista' exists in 'listb', False otherwise.
    """
    # check that all tags searched for are included in the list of runs to delete
    result = set(lista).issubset(set(listb))
    return result


def clean_spaces_around_delimiters(text, delimiter=","):
    """
    Removes extra spaces surrounding a specified delimiter in a string and strips newline characters.

    The function uses a regular expression to match any spaces before and after the delimiter,
    replacing them with a single instance of the delimiter. It also removes newline characters
    to ensure the output is a consolidated single-line string.

    Parameters:
    - text (str): The input string that potentially contains extra spaces around delimiters.
    - delimiter (str): The target delimiter around which spaces should be cleaned. Default is a comma (",").

    Returns:
    - str: The cleaned string with extra spaces removed around delimiters and newline characters stripped.
    """
    # Regex to match spaces before and after the delimiter
    pattern = rf"\s*{re.escape(delimiter)}\s*"
    # Replace with the delimiter only (stripping spaces)
    cleaned_text = re.sub(pattern, delimiter, text)
    cleaned_text = re.sub(r'\n', '', cleaned_text)
    return cleaned_text


def delete_run(runid, workspace_id, tower_access_token):
    """
    Deletes a run using the Nextflow Tower Cli (or 'tw') command-line tool.

    This function constructs a command to delete a specified run within a workspace.
    It then executes the command via subprocess.run and handles the output or errors.

    Parameters:
    - runid (list or similar iterable): The identifier(s) for the run. The command uses the first element.
    - workspace_id (str): The identifier for the workspace containing the run.
    - tower_access_token (str): The access token used for authentication with the 'tw' command.

    Returns:
    - str: The standard output from the command execution if successful.

    Notes:
    - If the command fails (non-zero exit status), the function captures and prints error details.
    - The function assumes that the 'tw' command is installed and available in the system's PATH.
    """
    tw_command = ["tw", f"-t {tower_access_token}", "runs", "delete", "--workspace", f"{workspace_id}", "-i", f"{runid[0]}"]
    print(tw_command)
    try:
        # Run the SeqeraKit command
        result = subprocess.run(tw_command, check=True, text=True, 
                                capture_output=True)

        # Print the output of the command
        print("Command Output:")
        print(result.stdout + result.stderr)
        return result.stdout

    except subprocess.CalledProcessError as e:
        print("Error running SeqeraKit command:")
        print("Error running SeqeraKit command:") 
        print(f"Return Code: {e.returncode}") 
        print(f"Command: {e.cmd}") 
        print(f"Output: {e.output}") 
        print(f"Error: {e.stderr}")


def download_run(runid, workspace_id, tower_access_token):
    tw_command = ["tw", f"-t {tower_access_token}", "runs", "dump", "--workspace", f"{workspace_id}", "-i", f"{runid}", f"--output=/tmp/{runid}.tar.gz"]
    print(tw_command)
    try:
        # Run the SeqeraKit command
        result = subprocess.run(tw_command, check=True, text=True, 
                                capture_output=True)

        # Print the output of the command
        print("Command Output:")
        print(result.stdout + result.stderr)
        return result.stdout

    except subprocess.CalledProcessError as e:
        print("Error running SeqeraKit command:")
        print("Error running SeqeraKit command:") 
        print(f"Return Code: {e.returncode}") 
        print(f"Command: {e.cmd}") 
        print(f"Output: {e.output}") 
        print(f"Error: {e.stderr}")   


def list_files_in_directory(directory: str) ->list:
    """
    Retrieves a list of files within the specified directory.

    Parameters:
    - directory (str): The path to the directory where files should be listed.

    Returns:
    - list: A list of filenames (as strings) found in the directory.
            If an error occurs (e.g., the directory does not exist), an empty list is returned.
    """
    try:
        files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
        return files
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

    
def open_tar(runid):
    """
    Extracts a tar.gz archive from the /tmp directory using the tar command.

    This function builds a command to extract a tar.gz archive whose name is based
    on the provided 'runid' and is assumed to be located in the /tmp directory.
    The extracted contents will be placed into /tmp. It executes this command using
    subprocess.run, capturing both standard output and standard error.

    Upon successful execution, the function prints and returns the command's standard output.
    If the command fails, it catches the error and prints detailed error information.

    Parameters:
    - runid (str): The identifier for the run. The tar file is expected to be at
                   /tmp/{runid}.tar.gz.

    Returns:
    - str: The standard output from the tar command if it executes successfully.
    """
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


def normalize_tw_output(query, page):
    """
    Normalizes and parses raw text output from a "tw" query into a structured pandas DataFrame.

    The function performs several steps to clean and format the data:
      1. Splits the raw input query by newline characters to work with individual lines.
      2. Checks for a minimal number of lines. If there are too few lines, it cleans a specific line
         (by removing ANSI escape codes), prints it, and asserts that it contains the expected
         message 'No pipeline runs found'. This likely handles cases where no data results were found.
      3. Removes unwanted header lines from the beginning and footer lines from the end of the output.
      4. Iterates over the remaining lines to:
         - Remove hidden ANSI escape codes (used for hyperlinks and formatting).
         - Clean extra spaces around delimiters using a custom helper function.
         - Replace occurrences of "None" with a space.
         - Trim any leading and trailing whitespace.
         - Remove artifacts such as "**" and other ANSI color formatting using regular expressions,
           and finally split each cleaned line by the "|" delimiter.
      5. Processes the first (header) row by replacing spaces with underscores.
      6. Constructs a pandas DataFrame using the header row as the column names and the subsequent rows as data.
      7. Normalizes the column names using Unicode normalization (NFKD) and further cleans them by
         removing any residual "**" markers.

    Parameters:
    - query (str): The raw text output from the "tw" query.
    - page: An additional parameter (not used directly in this function) that may be reserved for future
             enhancements or filtering.

    Returns:
    - pd.DataFrame: A DataFrame containing the cleaned and structured data.
    """

    modified_lines =query.split("\n")
    if len(modified_lines)<10:
        print(re.sub(r'\x1b]8;;.*?\x1b\\', '', modified_lines[3]))
        assert 'No pipeline runs found' in re.sub(r'\x1b]8;;.*?\x1b\\', '', modified_lines[3])
    del modified_lines[0:3]
    del modified_lines[1]
    del modified_lines[-5:-1]

    # Remove all hidden chars and tw specific formatting
    for i in range(len(modified_lines)):
        string = re.sub(r'\x1b]8;;.*?\x1b\\', '', modified_lines[i])
        modified = clean_spaces_around_delimiters(string, "|").replace("None", " ").lstrip().rstrip().replace("**", "")#.replace("|", "\t")
        modified_lines[i] = re.sub(r"\033\[[0-9;]*[mK]", "", modified).split("|")
    modified_lines[0] = [line.replace(" ", "_") for line in modified_lines[0]]
    new_df = pd.DataFrame(columns = modified_lines[0], data=modified_lines[1:]).dropna()
    new_df.columns = new_df.columns.str.normalize('NFKD') 
    new_df.columns = new_df.columns.str.replace(r"\*\*", "", regex=True)

    return new_df


def clean_failed_bs_fetch_nf(tower_access_token, workspace_id, page):
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
    

    "tw -t eyJ0aWQiOiAxMDYzOX0uMmVjOWRiOWI0OGE5OGQ0MWI3NWU5N2QxZDY1YzRjNjI5ZWM5NTcwMw== runs list --labels  --workspace 178360486244110"
    # Define the SeqeraKit command

    tw_command = ["tw", f"-t {tower_access_token}", "runs", "list", "--labels", "--workspace", f"{workspace_id}", "--page", f"{page}"]
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
    

def find_differences(string1: str, string2: str):
    """
    Identifies the differences between two strings.

    This function compares two strings character by character and returns a list
    of tuples representing the positions and differing characters. If one string
    is shorter than the other, the missing character is represented by None.

    Parameters:
    - string1 (str): The first input string.
    - string2 (str): The second input string.

    Returns:
    - List[Tuple[int, Optional[str], Optional[str]]]:
      A list where each tuple contains:
        (index, character from string1, character from string2)
      at positions where the two strings differ.
    """
    differences = []
    max_length = max(len(string1), len(string2))
    
    for i in range(max_length):
        char1 = string1[i] if i < len(string1) else None
        char2 = string2[i] if i < len(string2) else None
        
        if char1 != char2:
            differences.append((i, char1, char2))
    
    return differences


if __name__ == "__main__":
    # Loop over pages of pipeline runs.
    # The loop continues as long as the cleaned version of the 4th line of the query
    # output (after removing ANSI escape sequences and stripping whitespace) does not contain
    # the phrase "No pipeline runs found". This condition checks whether there are additional runs.
    while "No pipeline runs found" not in ansi_escape.sub("", query.split("\n")[3]).lstrip().rstrip():
        query = clean_failed_bs_fetch_nf(tower_access_token,workspace_id, page)

        if page == 1:
            norm_df = normalize_tw_output(query, page)
        elif query is None:
            exit(1)
        elif page >= 2 and "No pipeline runs found" not in ansi_escape.sub("", query.split("\n")[3]).lstrip().rstrip():# "No pipeline runs found":
            print(f"Page {page}: {query.split('\n')[0]}")
            new_df = normalize_tw_output(query, page)
            try:
                norm_df = pd.concat([norm_df, new_df], ignore_index=True)
            except KeyError:
                print(f"Pulled {page} pages due to missing data!")
        page += 1

    # Exit script if no run info is found
    if norm_df is None or norm_df.empty:   
        exit("No pipeline runs found with query")

    if days_from_now:
        norm_df["days_from_now"] = norm_df["Submit_Date"].apply(days_from_now)
    if status:
        norm_df = norm_df[norm_df["Status"].isin(status)]
    if run_name:
        norm_df = norm_df[norm_df["Run_Name"].isin(run_name)]
    if usr:
        norm_df = norm_df[norm_df["Username"].isin(usr)]
    if workflow:
        norm_df = norm_df[norm_df["Project_Name"].isin(workflow)]
    if days:
        norm_df = norm_df[norm_df["days_from_now"] > days]
    if remove_tags:
        # If list of tags provided, filter to only include runs for deletion 
        # that contain *ALL* tags in provided list of tags
        norm_df["tags_check"] = norm_df["Labels"].apply(lambda label: check_for_tags(label, remove_tags))
        norm_df = norm_df.loc[~norm_df["tags_check"]]


    ids_to_remove = norm_df["ID"].tolist()
    print(f"this would remove the following runs:\n {norm_df}")
    norm_list = norm_df.values.tolist()

    # If this is not a dry run, archive and then delete each run
    if not dryrun:
        for run in norm_list:
            if status != "CANCELLED":
                if pl == "production" or archive:
                    success = archive_run(run, norm_df.columns, pl,  workspace_id, tower_access_token)
            if status == "CANCELLED":
                success = True
            if success:
                # Only delete run if status is "CANCELLED" or if run has been successfully archived
                print(f"upload of {run} was successful, removing run from Seqera Cloud")
                delete_run(run, workspace_id, tower_access_token)
            
    else:
        print(f"The following seqs would be removed with this query:\n {ids_to_remove}")


