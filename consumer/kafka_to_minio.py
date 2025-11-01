import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import signal
import sys

load_dotenv()

required_vars = ['KAFKA_BOOTSTRAP', 'KAFKA_GROUP', 'MINIO_ENDPOINT', 'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY', 'MINIO_BUCKET']
for var in required_vars:
    if not os.getenv(var):
        print(f"❌ Missing environment variable: {var}")
        sys.exit(1)

# Kafka consumer - PAS de assign(), juste subscribe()
try:
    consumer = KafkaConsumer(
        'banking_server.public.customers',
        'banking_server.public.accounts',
        'banking_server.public.transactions',
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'banking-consumer-{datetime.now().timestamp()}',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        max_poll_records=100
    )
    print("✅ Connected to Kafka")
except Exception as e:
    print(f"❌ Kafka connection failed: {e}")
    sys.exit(1)

# MinIO client
try:
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
    )
    bucket = os.getenv("MINIO_BUCKET")
    if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
        s3.create_bucket(Bucket=bucket)
    print(f"✅ Connected to MinIO - Bucket: {bucket}")
except Exception as e:
    print(f"❌ MinIO connection failed: {e}")
    sys.exit(1)

def write_to_minio(table_name, records):
    if not records:
        return
    try:
        df = pd.DataFrame(records)
        date_str = datetime.now().strftime('%Y-%m-%d')
        file_path = f'/tmp/{table_name}_{date_str}.parquet'
        df.to_parquet(file_path, engine='pyarrow', index=False)
        s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
        s3.upload_file(file_path, bucket, s3_key)
        os.remove(file_path)
        print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')
    except Exception as e:
        print(f'❌ Error writing to MinIO: {e}')

batch_size = 50
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

def flush_buffers():
    print("\n🔄 Flushing remaining buffers...")
    for topic, records in buffer.items():
        if records:
            table_name = topic.split('.')[-1]
            write_to_minio(table_name, records)
    print("✅ All buffers flushed")

def signal_handler(sig, frame):
    print('\n⚠️ Interrupt received, shutting down...')
    flush_buffers()
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

print("✅ Listening for Kafka messages...")

try:
    for message in consumer:
        topic = message.topic
        event = message.value
        payload = event.get("payload", {})
        record = payload.get("after")
        
        if record:
            buffer[topic].append(record)
            print(f"[{topic}] Buffered record (buffer size: {len(buffer[topic])})")
            
            if len(buffer[topic]) >= batch_size:
                table_name = topic.split('.')[-1]
                write_to_minio(table_name, buffer[topic])
                buffer[topic] = []

except Exception as e:
    print(f"❌ Error consuming messages: {e}")
    import traceback
    traceback.print_exc()
finally:
    flush_buffers()
    consumer.close()
