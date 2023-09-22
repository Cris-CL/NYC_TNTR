# functions-framework==3.*
# flask==2.2.2
# pandas==1.5.1
# google-cloud-bigquery>=3.3.5
# gspread==5.7.2

from flask import Flask, request, jsonify
from uri import update_uri_sheet

app = Flask(__name__)
@app.route('/sheets_update', methods=['POST','GET'])
def process_sheet(request):

  if request.method=='GET':
      return '<h1> This is a webhook listener!</h1>'
  if request.method == 'POST':
    try:
      posted_data = request.json
      print("We have received a request")
    #   print(posted_data)
      column = posted_data["column"]
      starting_date = posted_data["date"]
      type_sh = posted_data["type"]

      print(f'type: {type_sh} col: {column} date:{starting_date}')

      update_uri_sheet(matching_column=column,start_date=starting_date,sheet_type=type_sh)

    except Exception as e:
      print(e)
      http_status='',400
    http_status=jsonify({'status':'success'}),200
  else:
    http_status='',400
  return http_status
