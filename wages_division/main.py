# functions-framework==3.*
# flask==2.2.2
from flask import Flask, request, jsonify
from process import main_process

app = Flask(__name__)


@app.route("/process_sheet", methods=["POST", "GET"])
def process_sheet(request):

    if request.method == "GET":
        return "<h1> This is a webhook listener!</h1>"
    if request.method == "POST":
        try:
            posted_data = request.json
            print(posted_data)
            print("We have received a request")
        except Exception as e:
            print(e)
        main_process("test", 2)
        # print("Date and time of update ====>")
        http_status = jsonify({"status": "success"}), 200
    else:
        http_status = "", 400
    return http_status
