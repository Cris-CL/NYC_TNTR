# flask==2.2.2
# pytz
# pandas==1.5.1
# google-cloud-bigquery==3.3.5
# google-cloud-storage==2.5.0
# fsspec==2022.11.0
# gcsfs==2022.11.0
# pandas-gbq==0.17.9
# numpy==1.23.4
# google-api-python-client==2.86.0

from datetime import datetime
from flask import Flask, request, jsonify
import pytz
from full_process import full_process


def get_timestamp(timezone_name):
  dt = datetime.now(pytz.timezone(timezone_name))
  timestamp = dt.strftime(("%Y-%m-%d %H:%M:%S"))
  return timestamp

app = Flask(__name__)
@app.route('/webhook', methods=['POST','GET'])
def drive_to_bucket(request):
  jap_timestamp = get_timestamp('Asia/Tokyo')
  gmt_timestamp = get_timestamp('GMT')
  if request.method=='GET':
      return '<h1> This is a webhook listener!</h1>'
  if request.method == 'POST':

    print(f"New request at {jap_timestamp} Japan Time")


    full_process()
    http_status=jsonify({'status':'success'}),200
    end_time = get_timestamp('Asia/Tokyo')
    print(f"Finished process at {end_time} Japan Time")
  else:
    http_status='',400
  return http_status
