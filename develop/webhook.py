# functions-framework==3.*
# flask==2.2.2
# pytz
from datetime import datetime
from flask import Flask, request, jsonify
import pytz

app = Flask(__name__)
@app.route('/webhook', methods=['POST','GET'])
def webhook(request):
  dt=datetime.now(pytz.timezone('Asia/Tokyo'))
  timestamp = dt.strftime(("%Y-%m-%d %H:%M:%S"))
  if request.method=='GET':
      return '<h1> This is a webhook listener!</h1>'
  if request.method == 'POST':
    posted_data=request.headers
    print("We have received a request =====>",posted_data)
    cur_date=timestamp
    print("Date and time of update ====>",cur_date)
    http_status=jsonify({'status':'success'}),200
  else:
    http_status='',400
  return http_status
