# functions-framework==3.4.0
# flask==2.2.3
# Werkzeug==2.3.7
# pytz
# requests

from datetime import datetime
from flask import Flask, request, jsonify
import pytz
import os
import requests

def get_timestamp(timezone_name):
  dt = datetime.now(pytz.timezone(timezone_name))
  timestamp = dt.strftime(("%Y-%m-%d %H:%M:%S"))
  return timestamp

app = Flask(__name__)
@app.route('/webhook', methods=['POST','GET'])
def webhook(request):
  jap_timestamp = get_timestamp('Asia/Tokyo')
  gmt_timestamp = get_timestamp('GMT')
  if request.method=='GET':
      return '<h1> This is a webhook listener!</h1>'
  if request.method == 'POST':
    posted_data=list(request.headers)
    print("We have received a request")
    print(f"GMT Time of request {gmt_timestamp}")
    print(f"Japan Time of request {jap_timestamp}")
    print(posted_data)

    ### If the message receibed is of a sync, return a success status and finish

    if posted_data[4][1]=="sync":
      http_status=jsonify({'status':'success'}),200
      return http_status

    cur_date=jap_timestamp
    print("Date and time of update ====>",cur_date)
    http_status=jsonify({'status':'success'}),200
    try:
      DRIVE_FUNCTION_URL = os.environ.get("DRIVE_FUNCTION_URL")
      requests.post(DRIVE_FUNCTION_URL,timeout=1)
    except requests.exceptions.ReadTimeout:
        print(f"POST request Send to {DRIVE_FUNCTION_URL}")
  else:
    http_status='',400
  return http_status
