import json
import boto3
import datetime
import uuid
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    BUCKET_NAME = os.environ['BUCKET_NAME']

    try:
        alert_data = json.loads(event['body'])

        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
        file_name = f"alert-{timestamp}-{str(uuid.uuid4())}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"raw/alerts/{file_name}",
            Body=json.dumps(alert_data),
            ContentType='application/json'
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Alert received and stored successfully!', 'fileName': file_name})
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Error processing request.', 'error': str(e)})
        }