from utils import *

def process_message(event, context):
    import base64
    import json

    # Extract the Pub/Sub message payload
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_data = json.loads(pubsub_message)

    # Extract file_name and file_uri from the message data
    file_name = message_data.get('file_name')
    file_uri = message_data.get('file_uri')

    if file_name and file_uri:
        print(f"Received message with file_name: {file_name}")
        print(f"File URI: {file_uri}")
        load_and_upload(file_uri,file_name)
    else:
        print("Received message with missing data")

    # Acknowledge the message
    # try:
    #     print(event)
    #     print(type(event))
    # except Exception as e:
    #     print(e)
    return
