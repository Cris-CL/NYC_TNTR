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

      hostess_name = posted_data["names"]
      type_request = posted_data["type"]
      attempt_number = posted_data["attempt"]
      print(hostess_name)
      if type_request == 'test':
        print('Test')
        return '<h1>Test run correctly</h1>'
      month = posted_data["month"]
      year = posted_data["year"]
      print(month)

      if type_request == 'regular':
        process_sheets_from_master(month,year,hostess_name)

      elif type_request == 'retry':
        print(f"trying retry for: {hostess_name}")
        process_sheets_from_master(month,year,hostess_name,attempts=attempt_number)
    except Exception as e:
      print(e)
      http_status='',400
    http_status=jsonify({'status':'success'}),200
  else:
    http_status='',400
  return http_status
