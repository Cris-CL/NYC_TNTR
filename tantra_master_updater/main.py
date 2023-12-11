# functions-framework==3.*
# flask==2.2.2
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
      if posted_data['type']=="online":
        from online_orders import update_online_sales
        print("Updating online orders")
        update_online_sales("Transfer")
    except Exception as e:
      print(e)
      http_status='',400
    http_status=jsonify({'status':'success'}),200
  else:
    http_status='',400
  return http_status
