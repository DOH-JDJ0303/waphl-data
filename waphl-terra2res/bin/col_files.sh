#!/bin/bash

FILE=$1

for S in $(cat ${FILE} | tail -n +2 | tr '\t' ',' | tr ' ' '_' )
do
   ID=$(echo ${S} | cut -f 1 -d ',')
   echo ${S} | tr ',' \\n | grep 'gs://' | awk -v id=${ID} -v OFS=',' '{print id,$1}' >> ${FILE%%.tsv}.csv
done
