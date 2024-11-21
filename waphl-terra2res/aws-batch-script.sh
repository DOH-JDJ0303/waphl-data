#!/bin/bash

#----- INPUTS -----#
TERRA_PROJECT=$1
TERRA_WORKSPACE=$2
TERRA_SUBMISSIONID=$3
TERRA_WORKFLOW=$4
TERRA_SUBMISSIONENTITY=$5
OUTDIR=$6
GCRED=$7

#----- REPORT INPUTS -----#
echo -e "\nInput Summary:"
echo "TERRA_PROJECT: ${TERRA_PROJECT}"
echo "TERRA_WORKSPACE: ${TERRA_WORKSPACE}"
echo "TERRA_SUBMISSIONID: ${TERRA_SUBMISSIONID}"
echo "TERRA_WORKFLOW: ${TERRA_WORKFLOW}"
echo "TERRA_SUBMISSIONENTITY: ${TERRA_SUBMISSIONENTITY}"
echo "S3_OUTDIR: ${S3_OUTDIR}"
echo "GCRED: ${GCRED}"
echo -e "\n"

#----- CONFIGURE GCLOUD -----#
aws s3 cp $GCRED google_credentials.json
export GOOGLE_APPLICATION_CREDENTIALS='$PWD/google_credentials.json'

#----- BUILD SAMPLESHEET -----#
echo -e "\nCreating Samplesheet:"
echo "project,workspace,workflow,run" > samplesheet.csv
echo "${TERRA_PROJECT},${TERRA_WORKSPACE},${TERRA_WORKFLOW},${TERRA_SUBMISSIONENTITY}" >> samplesheet.csv
cat samplesheet.csv && echo -e "\n"

#----- RUN PIPELINE -----#
echo -e "\nStarting pipeline:"
# run waphl-prod2res
CMD="nextflow run waphl-data/waphl-terra2res/main.nf --input samplesheet.csv --outdir s3://${OUTDIR}"
echo -e "\nCMD: ${CMD}\n"
${CMD}

#----- UPDATE LOG FILE -----#
echo -e "\nUpdating log file"
TIMESTAMP=$(cat .nextflow.log | grep "Files will be saved with timestamp:" | cut -f 4 -d ':' | tr -d ' ')
aws s3 cp .nextflow.log ${OUTDIR%%/}/logs/${TIMESTAMP}-waphl-terra2res.log

#----- CACHE SUBMISSION ID -----#
touch empty_file && aws s3 cp empty_file s3://${OUTDIR}/cache/terra/${TERRA_PROJECT}/${TERRA_WORKFLOW}/${TERRA_SUBMISSIONID}"

echo -e "\nPipeline Complete!"
