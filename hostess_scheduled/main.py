import os
from datetime import datetime
import requests

URL = os.environ.get("URL")

def send_request(test_var=True):
    if test_var == True:
        return
    if test_var == False:
        print("nada")
    month = datetime.now().month

    # now = int(datetime.utcnow().timestamp()*1e3)
    data_send = {
        "name": 'Test',
        "month": month
        }
    headers = {'Content-Type': 'application/json'}
    try:
      requests.post(URL,json=data_send,headers=headers,timeout=5)

    except requests.exceptions.ReadTimeout:
        print(f"POST request Send to {HOSTESS_UPDATE_URL} failed")

    return

def hostess_update(data, context):
    send_request(test_var=False)
    return
