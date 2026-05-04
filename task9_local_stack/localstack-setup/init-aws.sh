#!/bin/bash

echo "============================================="
echo "Initializing LocalStack Resources..."
echo "============================================="

# --- GLOBAL VARIABLES ---
REGION="us-east-1"
ACCOUNT="000000000000"
BUCKET_NAME="bike-data-2016-2020"
FUNCTION_NAME="BikeDataProcessor"
LAMBDA_ROLE="arn:aws:iam::${ACCOUNT}:role/lambda-role"
SNS_TOPIC_NAME="bike-file-notifications"

SNS_ARN="arn:aws:sns:${REGION}:${ACCOUNT}:${SNS_TOPIC_NAME}"
LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT}:function:${FUNCTION_NAME}"


# --- S3 BUCKET CREATION ---
echo "-> Creating S3 Bucket: $BUCKET_NAME"
awslocal s3 mb s3://$BUCKET_NAME


# --- DYNAMODB TABLES CREATION ---
echo "-> Creating DynamoDB Tables..."

awslocal dynamodb create-table \
    --table-name BikeMetricsDaily \
    --attribute-definitions AttributeName=date,AttributeType=S \
    --key-schema AttributeName=date,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=100

awslocal dynamodb create-table \
    --table-name BikeMetricsMonthly \
    --attribute-definitions AttributeName=month_id,AttributeType=S \
    --key-schema AttributeName=month_id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=100


# --- LAMBDA PACKAGING & DEPLOYMENT ---
echo "-> Packaging Pandas and Lambda Function..."
rm -rf /tmp/lambda_build
mkdir -p /tmp/lambda_build
cd /tmp/lambda_build

# Copy the python script
cp /var/task/lambda_code/lambda_function.py .

# Install Pandas for Amazon Linux (Python 3.10)
echo "Downloading Linux binaries for Pandas..."
pip install pandas -t . \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.10 \
    --only-binary=:all: \
    --no-cache-dir > /dev/null 2>&1

# Zip it all up
echo "Zipping deployment package..."
zip -r function.zip . > /dev/null

echo "-> Creating Lambda Function: $FUNCTION_NAME..."
awslocal lambda create-function \
    --function-name $FUNCTION_NAME \
    --runtime python3.10 \
    --handler lambda_function.lambda_handler \
    --role $LAMBDA_ROLE \
    --zip-file fileb://function.zip \
    --timeout 900 \
    --memory-size 1024


# --- EVENT-DRIVEN ARCHITECTURE (S3 -> SNS -> LAMBDA) ---
echo "-> Setting up SNS and S3 Event Notifications..."

# Create the SNS Topic
awslocal sns create-topic --name $SNS_TOPIC_NAME

# Give SNS permission to invoke the Lambda
awslocal lambda add-permission \
    --function-name $FUNCTION_NAME \
    --action lambda:InvokeFunction \
    --statement-id AllowSNSInvoke \
    --principal sns.amazonaws.com \
    --source-arn $SNS_ARN

# Subscribe Lambda to the SNS Topic
awslocal sns subscribe \
    --topic-arn $SNS_ARN \
    --protocol lambda \
    --notification-endpoint $LAMBDA_ARN

# Create the S3 Event Filter Configuration (Triggers on metrics/*.csv)
cat <<EOF > /tmp/s3-notification.json
{
    "TopicConfigurations": [
        {
            "TopicArn": "${SNS_ARN}",
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {"Name": "prefix", "Value": "metrics/"},
                        {"Name": "suffix", "Value": ".csv"}
                    ]
                }
            }
        }
    ]
}
EOF

# Apply the Event Notification trigger to the S3 Bucket
awslocal s3api put-bucket-notification-configuration \
    --bucket $BUCKET_NAME \
    --notification-configuration file:///tmp/s3-notification.json

echo "============================================="
echo "LocalStack initialization complete!"
echo "Pipeline is ready for Airflow ingestion."
echo "============================================="