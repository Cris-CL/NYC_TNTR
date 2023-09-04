# functions-framework==3.*
# google-cloud-storage
# google-cloud-pubsub

from google.cloud import storage
from google.cloud import pubsub_v1
import os
import json

storage_client = storage.Client()
pubsub_publisher = pubsub_v1.PublisherClient()

PROJECT_NAME = os.environ.get("PROJECT_NAME")
TOPIC_NAME = os.environ.get("TOPIC_NAME")

def send_message(event, context):
    bucket_name = event['bucket']
    file_name = event['name']
    file_uri = f'gs://{bucket_name}/{file_name}'

    topic_name = f'projects/{PROJECT_NAME}/topics/{TOPIC_NAME}'  # Replace with your actual Pub/Sub topic name

    message = {
        'file_name': file_name,
        'file_uri': file_uri
    }

    message_data = json.dumps(message).encode('utf-8')

    try:
        pubsub_publisher.publish(topic_name, data=message_data)
        print(f'Message published for file: {file_name}')
    except Exception as e:
        print('Error publishing message:', e)
    return
