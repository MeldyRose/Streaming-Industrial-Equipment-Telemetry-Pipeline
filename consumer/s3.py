from kafka import KafkaConsumer
from dotenv import load_dotenv

import boto3
import json
import os
import time

from datetime import datetime, timezone

from config.equipment_config import (KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

load_dotenv()  # Load environment variables from .env file

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")  # Replace with your S3 bucket name
AWS_REGION = os.getenv("AWS_REGION")  # Replace with your AWS region

BATCH_SIZE = 100  # Number of messages to batch before uploading to S3
BATCH_INTERVAL_SECONDS = 40  # Time interval to batch messages before uploading to S3

MAX_RUN_TIME_SECONDS = 300
MAX_MESSAGES = 1000

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
)

def upload_batch_to_s3(events):
    if not events:
        return

    timestamp = datetime.now(timezone.utc)

    #Create a unique S3 key for the batch file based on the current timestamp
    #Make sure it is bronze stage of data quality, which means it is raw data that has not been processed or cleaned yet. The bronze stage is the first stage in a typical data lake architecture, where raw data is ingested and stored before any transformations or processing are applied.
    s3_key = (
        f"bronze/machine_telemetry_{timestamp.strftime('%Y%m%d-%H%M%S')}.json"
    )

    body = "\n".join(
        json.dumps(event) for event in events
    )

    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    print(f"Uploaded {len(events)} events to s3://{S3_BUCKET_NAME}/{s3_key}")

#Limiting the number of messages in each batch and the time interval. If either condition is met, the batch will be uploaded to S3.
#It is limited due to cost and performance considerations. Uploading too frequently or with too many messages can lead to increased costs and potential performance issues. By batching messages, we can reduce the number of S3 PUT requests and optimize the overall performance of the system.
buffer = []

buffer_start_time = time.monotonic()
consumer_start_time = time.monotonic()

total_messages_processed = 0

try:

    for message in consumer:
        buffer.append(message.value)
        total_messages_processed += 1

        elapsed_time = time.monotonic() - buffer_start_time
        total_run_time = time.monotonic() - consumer_start_time

        if len(buffer) >= BATCH_SIZE or elapsed_time >= BATCH_INTERVAL_SECONDS:
            upload_batch_to_s3(buffer)
            buffer.clear()
            buffer_start_time = time.monotonic()

        if total_run_time >= MAX_RUN_TIME_SECONDS or total_messages_processed >= MAX_MESSAGES:
            print("Max run time or max messages reached. Exiting...")
            break

finally:
    if buffer:
        upload_batch_to_s3(buffer)
        buffer.clear()
    consumer.close()
    print("Consumer closed. Exiting...")