import json
import boto3
import pymysql
import datetime
from botocore.exceptions import ClientError

# Inisialisasi klien AWS
ssm = boto3.client('ssm')
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Ambil konfigurasi dari AWS SSM Parameter Store
def get_ssm_parameter(name):
    try:
        response = ssm.get_parameter(Name=name, WithDecryption=True)
        return response['Parameter']['Value']
    except ClientError as e:
        print(f"Error retrieving parameter {name}: {e}")
        return None


# Fungsi untuk menghubungkan ke MariaDB di RDS
def connect_rds():
    # Ambil parameter dari SSM
    RDS_HOST = get_ssm_parameter('/tryout1/RDSHost')
    RDS_USER = get_ssm_parameter('/tryout1/RDSUsername')
    RDS_PASSWORD = get_ssm_parameter('/tryout1/RDSPassword')
    RDS_DATABASE = get_ssm_parameter('/tryout1/RDSDatabase')
    try:
        conn = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASSWORD,
            database=RDS_DATABASE,
            cursorclass=pymysql.cursors.DictCursor
        )
        return conn
    except Exception as e:
        print(f"Error connecting to RDS: {e}")
        return None

# Lambda handler
def lambda_handler(event, context):
    try:
        # Ambil data dari DynamoDB
        data_list = fetch_data_from_dynamodb()

        if not data_list:
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "No new data to process"})
            }

        # Proses data
        processed_data = process_data(data_list)

        # Simpan data ke MariaDB di RDS
        save_to_rds(processed_data)

        # Simpan data ke S3
        save_to_s3(processed_data)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data processed and saved successfully"})
        }

    except Exception as e:
        print(f"Error in Lambda function: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal server error"})
        }

# Fungsi untuk mengambil data dari DynamoDB
def fetch_data_from_dynamodb():
    DYNAMODB_TABLE = get_ssm_parameter('/tryout1/DynamoDBTableName')

    table = dynamodb.Table(DYNAMODB_TABLE)
    response = table.scan()
    items = response.get('Items', [])
    return items

# Fungsi untuk memproses data (misalnya konversi suhu Fahrenheit ke Celsius)
def process_data(data_list):
    processed_data = []
    for item in data_list:
        if item.get("event_type") == "temperature":
            item["value_celsius"] = round((float(item["value"]) - 32) * 5/9, 2)
        processed_data.append(item)
    return processed_data

# Fungsi untuk menyimpan data ke MariaDB di RDS
def save_to_rds(data_list):
    RDS_TABLE = get_ssm_parameter('/tryout1/RDSTable')
    conn = connect_rds()
    if not conn:
        print("Failed to connect to RDS")
        return

    try:
        with conn.cursor() as cursor:
            for data in data_list:
                datetime_str = data['timestamp']
                datetime_obj = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
                formatted_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')                
                sql = f"""
                INSERT INTO {RDS_TABLE} (device_id, event_type, value, value_celsius, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    data["device_id"],
                    data["event_type"],
                    data["value"],
                    data.get("value_celsius"),
                    formatted_datetime
                ))
        conn.commit()
    except Exception as e:
        print(f"Error inserting data to RDS: {e}")
    finally:
        conn.close()

# Fungsi untuk menyimpan data ke S3
def save_to_s3(data_list):
    S3_BUCKET_NAME = get_ssm_parameter('/tryout1/S3BucketName')

    file_name = f"data-mateng/processed_events_{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(data_list),
        ContentType="application/json"
    )
