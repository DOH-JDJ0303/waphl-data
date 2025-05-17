# waphl-data General Lambda Build Guide

### Table of Contents
1. [Enter into directory with relevant Dockerfile](#enter-into-directory-with-dockerfile-for-lambda-function-you-wish-to-edit)
2. [Build the Image](#build-the-image)
3. [Testing](#testing)
4. [Host Image on AWS ECR](#host-image-on-aws-ecr)
5. [Create Lambda Function](#create-lambda-function)
6. [Automate Running of Lambda Function](#automate-running-of-lambda-function)


<br><br>

## Enter into directory with Dockerfile for Lambda Function you wish to edit
```
cd waphl-seq2arch/seq2archBuilder-bacteria
```

<br>

## Build the Image
```
docker buildx build --platform linux/amd64 --provenance=false \
--progress=plain --no-cache -t  <dockerimage_name>:<version> .
```
<br>

## Testing

Run the docker image locally to test before generating lambda function 
(this assumes aws lambda base image was used as final image layer)
```
docker run --rm --platform linux/amd64 -p 9000:8080 -v ~/.aws:/root/.aws <repository_name>:<version>
```

Open another terminal and verify the expected output with:
```
curl "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```
(copy this verbatim, *do not* change 2015-03-31 to the current date as its a standard input)

<br>

## Host Image on AWS ECR
Login to AWS ECR
```
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 398869308272.dkr.ecr.us-west-2.amazonaws.com
```

*If* a repository does not exist yet for your tool, then you will need to create a repository
```
aws ecr create-repository --repository-name <repository_name> --region us-west-2
```


Tag image for hosting on aws
```
docker tag <repository_name>:<version> <AWS_Account_ID>.dkr.ecr.us-west-2.amazonaws.com/<repository_name>:<version>
```

Push docker image to AWS
```
docker push <AWS_Account_ID>.dkr.ecr.us-west-2.amazonaws.com/<repository_name>:<version>
```
<br>

## Create lambda function
```
aws lambda create-function --function-name <repository_name> --package-type Image --code ImageUri=<AWS_Account_ID>.dkr.ecr.us-west-2.amazonaws.com/<repository_name>:<version> --role arn:aws:iam::<AWS_Account_ID>:role/generalLambdaRole --region us-west-2
```
<br>

## Automate Running of Lambda Function
Create Trigger (based on time) for Lambda Function - 
this example would trigger the rule as the start of every Sunday
```
aws events put-rule --name "SundayLambdaTrigger" --schedule-expression "cron(0 0 ? * SUN *)"
```

Add Lambda function as target of trigger
```
aws events put-targets --rule "WeeklyLambdaTrigger" --targets "Id"="1","Arn"="arn:aws:lambda:REGION:ACCOUNT_ID:function:YOUR_LAMBDA_FUNCTION_NAME"
```

Give the trigger permissions to invoke the trigger
```
aws lambda add-permission --function-name YOUR_LAMBDA_FUNCTION_NAME \
      --statement-id "EventBridgeInvoke" --action "lambda:InvokeFunction" \
      --principal events.amazonaws.com --source-arn arn:aws:events:REGION:ACCOUNT_ID:rule/WeeklyLambdaTrigger
```
