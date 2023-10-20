# functions-framework==3.*
# flask==2.2.2
from flask import Flask, request, jsonify
from process import process_sheets_from_master

app = Flask(__name__)
@app.route('/process_sheet', methods=['POST','GET'])
def process_sheet(request):

  if request.method=='GET':
      return '<h1> This is a webhook listener!</h1>'
  if request.method == 'POST':
    try:
      posted_data = request.json
      print("We have received a request")
      print(posted_data)

      hostess_name = posted_data["name"]
      print(hostess_name)
      month = posted_data["month"]
      print(month)

      # main_process(hostess_name,month)
      process_sheets_from_master(month)
    except Exception as e:
      print(e)
      http_status='',400
    # main_process("test",2)
    # print("Date and time of update ====>")
    http_status=jsonify({'status':'success'}),200
  else:
    http_status='',400
  return http_status
