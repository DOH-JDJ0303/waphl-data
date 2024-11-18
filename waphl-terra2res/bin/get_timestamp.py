#!/usr/bin/env python

import os
import argparse
from google.cloud import storage

#----- ARGUMENTS -----#
parser = argparse.ArgumentParser(
                    prog='get_timestamp.py',
                    description='Get Google object timestamp.')
parser.add_argument('-i',
                    '--input',
                    help = 'Google file')
parser.add_argument('-p',
                    '--project',
                    help = 'Google project')
args = parser.parse_args()

# set Google project
os.environ.setdefault("GCLOUD_PROJECT", args.project)

#------ GET TIMESTAMP ------#
gs_path = args.input.split('gs://')[1] # bucket/path/to/file.txt
gs_bucket = gs_path.split('/')[0] # bucket
gs_blob = '/'.join(gs_path.split('/')[1:]) # path/to/file.txt

storage_client = storage.Client()
bucket = storage_client.bucket(gs_bucket)
blob = bucket.get_blob(gs_blob)
file_time = None
if blob.exists():
    file_time = blob.time_created.timestamp()

# write file
results = [ str(args.input), str(file_time) ]
with open("timestamp.csv", "w") as outfile:
    outfile.write(",".join(results))