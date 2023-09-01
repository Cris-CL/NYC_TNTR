# functions-framework==3.*
# flask==2.2.2
# pytz
from datetime import datetime
from flask import Flask, request, jsonify
import pytz

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
    print(type(posted_data))
    print("We have received a request")
    print(f"GMT Time of request {gmt_timestamp}")
    print(f"Japan Time of request {jap_timestamp}")
    print(posted_data)
    http_status=jsonify({'status':'success'}),200
  else:
    http_status='',400
  return http_status
