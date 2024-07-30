#!/bin/sh

# QA
S3_BUCKET_SCRIPTS="aws-glue-assets-637423333664-us-east-2"
S3_PATH_SOURCE="s3://laive/"
S3_PATH_TRANSFORM="s3://laive-poc-stage/stage-qa/"
#GLUE_RDS_CONNECTION="connection-wally-rdsaurora"
PROFILE="laive_poc_datalake"
WHEEL_FILE="glue_utils-0.1-py3-none-any.whl"
GLUE_ROLE_NAME="AWS_Glue"
TAG_ENV="qas"
TAG_PROJECT="laive-datalake-qas"

# Compile Wheel files
python setup.py bdist_wheel

# Deploy scripts to S3
aws s3 cp dist/$WHEEL_FILE s3://$S3_BUCKET_SCRIPTS/scripts/stage/ --profile $PROFILE
aws s3 cp etl-config.json s3://$S3_BUCKET_SCRIPTS/scripts/stage/ --profile $PROFILE
aws s3 cp job_masters_stage_qa.py s3://$S3_BUCKET_SCRIPTS/scripts/stage/ --profile $PROFILE

# Delete Glue jobs if exist
aws glue delete-job --job-name job_masters_stage_qa --profile $PROFILE

# Create Glue jobs

aws glue create-job --name job_masters_stage_qa --role $GLUE_ROLE_NAME --command \
    "{\"Name\" :  \"glueetl\", \"ScriptLocation\" : \"s3://${S3_BUCKET_SCRIPTS}/scripts/stage/job_masters_stage_qa.py\", \"PythonVersion\": \"3\"}" \
    --glue-version 3.0 --execution-class STANDARD \
    --default-arguments "{\"--extra-py-files\": \"s3://${S3_BUCKET_SCRIPTS}/scripts/stage/${WHEEL_FILE}\", \"--enable-metrics\": \"true\", \"--enable-job-insights\": \"true\", \"--enable-spark-ui\": \"true\", \"--job-language\": \"python\", \"--spark-event-logs-path\": \"s3://${S3_BUCKET_SCRIPTS}/sparkHistoryLogs/\", \"--enable-glue-datacatalog\": \"true\", \"--job-bookmark-option\": \"job-bookmark-disable\", \"--TempDir\": \"s3://${S3_BUCKET_SCRIPTS}/temporary/\", \"--bucket_name_scripts\": \"${S3_BUCKET_SCRIPTS}\", \"--s3_path_source\": \"${S3_PATH_SOURCE}\", \"--s3_path_transform\": \"${S3_PATH_TRANSFORM}\"}" \
    --tags Env=$TAG_ENV,Project=$TAG_PROJECT --profile $PROFILE --region us-east-2


# Execute Glue Job

aws glue start-job-run --job-name job_masters_stage_qa \
    --number-of-workers 10 --worker-type G.1X \
    --profile $PROFILE --region us-east-2