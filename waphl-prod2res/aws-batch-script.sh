#!/bin/bash

#----- INPUTS -----#
S3_BUCKET=$1
S3_KEY=$2
OUTDIR=$3

#----- REPORT INPUTS -----#
echo -e "\nInput Summary:"
echo "S3_BUCKET: ${S3_BUCKET}"
echo "S3_KEY: ${S3_KEY}"
echo "OUTDIR: ${OUTDIR}"
echo -e "\n"

#----- DETERMINE WORKFLOW & RUN DIRECTORY -----#
# use file name to determine the workflow and how the run
# name should be structured within the object key path
echo -e "\nDetermining workflow:"
FILENAME=$(basename "${S3_KEY}") && echo "FILENAME: ${FILENAME}"
case "${FILENAME}" in
    "Phoenix_Summary.tsv")
        WORKFLOW="phoenix"
        RUNDIR=$(echo ${S3_KEY} | tr '/' '\n' | head -n -1 | tr '\n' '/')
        ;;
    *)
        echo "Error: ${FILENAME} does not match 'Phoenix_Summary.tsv'." && exit 1
        ;;
esac
echo "WORKFLOW: ${WORKFLOW}\n"

#----- BUILD SAMPLESHEET -----#
echo -e "\nCreating Samplesheet:"
echo "workflow,run" > samplesheet.csv
echo "${WORKFLOW},s3://${S3_BUCKET}/${RUNDIR}" >> samplesheet.csv
cat samplesheet.csv && echo -e "\n"

#----- RUN PIPELINE -----#
echo -e "\nStarting pipeline:"
# run waphl-prod2res
CMD="nextflow run waphl-data/waphl-prod2res/main.nf --input samplesheet.csv --retention_schema waphl-prod2res/retention-schemes.config --outdir ${OUTDIR}"
echo -e "\nCMD: ${CMD}\n"
${CMD}

#----- RUN PIPELINE -----#
echo -e "\nUpdating log file"
TIMESTAMP=$(cat .nextflow.log | grep "Files will be saved with timestamp:" | cut -f 4 -d ':' | tr -d ' ')
aws s3 cp .nextflow.log ${OUTDIR%%/}/logs/${TIMESTAMP}-waphl-prod2res.log

echo -e "\nPipeline Complete!"
