#!/bin/bash

#----- INPUTS -----#
BUCKET=$1
KEY=$2

#----- REPORT INPUTS -----#
echo -e "\nInput Summary:"
echo "BUCKET: ${BUCKET}"
echo "KEY: ${KEY}"
echo -e "\n"

#----- CONFIGURE -----#
DATADIR="s3://${BUCKET}/${KEY}"

#----- RUN PIPELINE -----#
echo -e "\nStarting pipeline:"
# run waphl-prod2res
CMD="nextflow run waphl-data/waphl-res2tbls/main.nf --data ${DATADIR}"
echo -e "\nCMD: ${CMD}\n"
${CMD}
