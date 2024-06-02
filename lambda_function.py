import json
import boto3
import pandas as pd  

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = "arn:aws:sns:us-east-1:058264373160:notification-alert-sns"

def lambda_handler(event, context):

    print(event)
    try:
        s3_bucket_name = event['Records'][0]['s3']['bukcet']['name']
        s3_object_key = event['Records'][0]['s3']['object']['key']
        print(s3_bucket_name)
        print(s3_object_key)
        resp = s3_client.get_object(Bucket = s3_bucket_name, Key = s3_object_key)
        print(resp["Body"])
        df_s3_bucket = pd.read_csv(resp["Body"], sep = ',')
        print(df_s3_bucket.head())

        message = "Successfully received the file !!! {}".format("s3://" + s3_bucket_name + "/" + s3_object_key)
        sns_client.publish(Subject = "SUCCESS - Daily Data received", TargetArn = sns_arn, Message = message, MessageStructure = 'text')

    except Exception as err:
        message = "Not able to receive the file !!! {}".format("s3://" + s3_bucket_name + "/" + s3_object_key)
        sns_client.publish(Subject = "FAILED - Daily Data received", TargetArn = sns_arn, Message = message, MessageStructure = 'text')
