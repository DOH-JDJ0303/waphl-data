#!/bin/bash

set -e

STACK_VERSION=1.0.9
# PROD_BUCKET=""
# RES_BUCKET=""
# AWS_ECR=""

cat <<EOF > parameters-prod2res.json
[
  {
    "ParameterKey": "SourceBucket",
    "ParameterValue": "${PROD_BUCKET}"
  },
  {
    "ParameterKey": "DestBucket",
    "ParameterValue": "${RES_BUCKET}"
  },
  {
    "ParameterKey": "GoogleCloudCredentials",
    "ParameterValue": "s3://${RES_BUCKET}/assets/google-cloud/application_default_credentials.json"
  },
  {
    "ParameterKey": "SubnetIds",
    "ParameterValue": "subnet-c73c46ec,subnet-6be31313,subnet-433af91e,subnet-615d402a"
  },
  {
    "ParameterKey": "SecurityGroupIds",
    "ParameterValue": "sg-f59931a8"
  },
  {
    "ParameterKey": "TerraWorkspaces",
    "ParameterValue": "general_bacterial_wgs,hai"
  },
  {
    "ParameterKey": "TerraProject",
    "ParameterValue": "waphl-covid-billing-1"
  },
  {
    "ParameterKey": "GatherFunctionImageTag",
    "ParameterValue": "${STACK_VERSION}"
  },
  {
    "ParameterKey": "CopyFunctionImageTag",
    "ParameterValue": "${STACK_VERSION}"
  },
  {
    "ParameterKey": "TerraGatherFunctionImageTag",
    "ParameterValue": "${STACK_VERSION}"
  },
  {
    "ParameterKey": "TerraCopyBatchImageTag",
    "ParameterValue": "${STACK_VERSION}"
  }
]
EOF

# ----- Build Docker Images ----
cd waphl-data/

# ----- Base Images -----
docker build \
    -t waphl-data:latest \
    -f dockerfiles/Dockerfile.base .

docker build \
    -t waphl-data-lambda:latest \
    -f dockerfiles/Dockerfile.lambda .

# ----- Prod2Res-Gather -----
docker build \
    -t "${AWS_ECR}/prod2res-gather:${STACK_VERSION}" \
    -t ${AWS_ECR}/prod2res-gather:latest \
    -f dockerfiles/Dockerfile.prod2res-gather .
docker push ${AWS_ECR}/prod2res-gather:${STACK_VERSION}
docker push ${AWS_ECR}/prod2res-gather:latest

# ----- Prod2Res-Cp -----
docker build \
    -t ${AWS_ECR}/prod2res-cp:${STACK_VERSION} \
    -t ${AWS_ECR}/prod2res-cp:latest \
    -f dockerfiles/Dockerfile.prod2res-cp .
docker push ${AWS_ECR}/prod2res-cp:${STACK_VERSION}
docker push ${AWS_ECR}/prod2res-cp:latest

# ----- Prod2Res-Terra-Gather -----
docker build \
    -t ${AWS_ECR}/prod2res-terra-gather:${STACK_VERSION} \
    -t ${AWS_ECR}/prod2res-terra-gather:latest \
    -f dockerfiles/Dockerfile.prod2res-terra-gather .
docker push ${AWS_ECR}/prod2res-terra-gather:${STACK_VERSION}
docker push ${AWS_ECR}/prod2res-terra-gather:latest

# ----- Prod2Res-Terra-Gather -----
docker build \
    -t ${AWS_ECR}/prod2res-terra-cp:${STACK_VERSION} \
    -t ${AWS_ECR}/prod2res-terra-cp:latest \
    -f dockerfiles/Dockerfile.prod2res-terra-cp .
docker push ${AWS_ECR}/prod2res-terra-cp:${STACK_VERSION}
docker push ${AWS_ECR}/prod2res-terra-cp:latest

cd ../

# ----- Update Stack -----
aws cloudformation update-stack \
    --template-body file://waphl-data/stacks/cloudformation.yml \
    --stack-name waphl-prod2res \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameters file://parameters-prod2res.json