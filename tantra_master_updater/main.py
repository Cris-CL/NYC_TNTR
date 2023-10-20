# functions-framework==3.*
# pytz
# google-cloud-bigquery==3.3.5
# fsspec==2022.11.0
# gcsfs==2022.11.0
# gspread==5.7.2
# pandas-gbq==0.17.9
# numpy==1.23.4
# openpyxl==3.0.10
# pyarrow>=9.0.0
# flask==2.2.3
# Werkzeug==2.3.7
# requests
# pandas==1.5.1


from flask import Flask, request, jsonify


from time import sleep

app = Flask(__name__)
@app.route('/update_master', methods=['POST','GET'])
def update_master(request):

  if request.method=='GET':
      return '<h1> This is a webhook listener!</h1>'
  if request.method == 'POST':
    try:
      posted_data = request.json
      print("We have received a request")
      print(posted_data)
      if posted_data["type"]=="extra":
        from extra import update_extra
        update_extra("Extra")
        sleep(100)
      if posted_data["type"]=="wages":
        from wages import update_wages
        print("Updating wages")
        sheet_name = posted_data["sheetName"]
        try:
          update_wages(sheet_name)
        except Exception as e:
          print(e)
      if posted_data["type"]=="prices":
        from prices import update_prices
        print("Updating prices")
        sheet_name = posted_data["sheetName"]
        try:
          update_prices(sheet_name)
        except Exception as e:
          print(e)
    except Exception as e:
      print(e)
      http_status='',400
    http_status=jsonify({'status':'success'}),200
  else:
    http_status='',400
  return http_status
