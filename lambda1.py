import json
import boto3
import datetime
from botocore.exceptions import ClientError

# Inisialisasi klien AWS
ssm = boto3.client('ssm')
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
sns = boto3.client('sns')

# Ambil konfigurasi dari AWS SSM Parameter Store
def get_ssm_parameter(name):
    try:
        response = ssm.get_parameter(Name=name, WithDecryption=True)
        return response['Parameter']['Value']
    except ClientError as e:
        print(f"Error retrieving parameter {name}: {e}")
        return None


# Lambda handler untuk menerima data dari API Gateway
def lambda_handler(event, context):
    try:
        # Jika event dari API Gateway, ambil body request
        if 'body' in event:
            event = json.loads(event['body'])

        # Extract data dari event
        device_id = event.get("device_id")
        event_type = event.get("event_type")
        value = event.get("value")
        timestamp = event.get("timestamp")

        if not (device_id and event_type and value and timestamp):
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Invalid event data"})
            }

        # Simpan ke DynamoDB
        save_to_dynamodb(device_id, event_type, value, timestamp)

        # Simpan ke S3
        save_to_s3(device_id, event_type, value, timestamp)

        # Kirim notifikasi ke SNS jika suhu tinggi (> 100°C)
        if event_type == "temperature" and value > 100:
            send_sns_notification(device_id, value)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Event processed successfully"})
        }
    
    except Exception as e:
        print(f"Error processing event: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal server error"})
        }

# Fungsi untuk menyimpan data ke DynamoDB
def save_to_dynamodb(device_id, event_type, value, timestamp):
    DYNAMODB_TABLE = get_ssm_parameter('/tryout1/DynamoDBTableName')
    table = dynamodb.Table(DYNAMODB_TABLE)
    response = table.put_item(
        Item={
            'device_id': device_id,
            'event_type': event_type,
            'value': value,
            'timestamp': timestamp
        }
    )
    print(f"Data saved to DynamoDB: {response}")

# Fungsi untuk menyimpan data ke S3
def save_to_s3(device_id, event_type, value, timestamp):
    S3_BUCKET_NAME = get_ssm_parameter('/tryout1/S3BucketName')

    file_name = f"data-mentah/{device_id}-{timestamp.replace(':', '-')}.json"
    event_data = {
        "device_id": device_id,
        "event_type": event_type,
        "value": value,
        "timestamp": timestamp
    }
    response = s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(event_data),
        ContentType='application/json'
    )
    print(f"Event saved to S3: {response}")

# Fungsi untuk mengirim notifikasi ke SNS jika suhu kritis
def send_sns_notification(device_id, value):
    SNS_TOPIC_ARN = get_ssm_parameter('/tryout1/SNSTopicARN')

    message = f"🚨 WARNING: High temperature detected!\nDevice: {device_id}\nTemperature: {value}°C"
    response = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject="High Temperature Alert!"
    )
    print(f"SNS notification sent: {response}")
