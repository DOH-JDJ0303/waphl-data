#!/bin/bash

#----- INPUTS -----#
S3_BUCKET=$1
S3_KEY=$2
OUTDIR=$3

#----- REPORT INPUTS -----#
echo "S3_BUCKET: ${S3_BUCKET}"
echo "S3_KEY: ${S3_KEY}"
echo "OUTDIR: ${OUTDIR}"

#----- DETERMINE WORKFLOW & RUN DIRECTORY -----#
# use file name to determine the workflow and how the run
# name should be structured within the object key path
FILENAME=$(basename "${S3_KEY}")
case "${FILENAME}" in
    "Phoenix_Summary.tsv")
        WORKFLOW="phoenix"
        RUNDIR=$(echo ${S3_KEY} | tr '/' '\n' | head -n -1 | tr '\n' '/')
        ;;
    *)
        echo "Filename does not match an expected pattern" && exit 1
        ;;
esac

#----- BUILD SAMPLESHEET -----#
echo "workflow,run" > samplesheet.csv
echo "${WORKFLOW},s3://${S3_BUCKET}/${RUNDIR}" >> samplesheet.csv
echo -e "\nRunning samplesheet:" && cat samplesheet.csv && echo -e "\n"

#----- RUN PIPELINE -----#
# clone waphl-data repo
rm -r waphl-data > /dev/null 2>&1
git clone https://github.com/DOH-JDJ0303/waphl-data.git && ls waphl-data/
# run waphl-prod2res
CMD="nextflow run waphl-data/waphl-prod2res/main.nf --input samplesheet.csv --retention_schema waphl-prod2res/retention-schemes.config --outdir ${OUTDIR}"
echo -e "\nCMD: ${CMD}\n"
${CMD}

#----- RUN PIPELINE -----#
TIMESTAMP=$(cat .nextflow.log | grep "Files will be saved with timestamp:" | cut -f 4 -d ':' | tr -d ' ')
aws s3 cp .nextflow.log ${OUTDIR%%/}/logs/${TIMESTAMP}-waphl-prod2res.log
