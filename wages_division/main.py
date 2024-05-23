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
      if hostess_name == 'Test':
        print('Test')
        return '<h1>Test run correctly</h1>'
      month = posted_data["month"]
      year = posted_data["year"]
      print(month)

      if hostess_name == 'All':
        process_sheets_from_master(month,year,hostess_name)
      elif "[" in hostess_name:
        print("placeholder")
    except Exception as e:
      print(e)
      http_status='',400
    # main_process("test",2)
    # print("Date and time of update ====>")
    http_status=jsonify({'status':'success'}),200
  else:
    http_status='',400
  return http_status
