# functions-framework==3.*
# flask==2.2.2
from flask import Flask, request, jsonify
from process import process_sheets_from_master

app = Flask(__name__)


@app.route("/process_sheet", methods=["POST", "GET"])
def process_sheet(request):
    """
    Function that receives the daily update message, and triggers the whole process,
    the type of messages are normal update (for the current and previous months)
    for all of the hostess, and a retry update with the failed updates from the
    previous excecution.

    Args:
        request (str): Sub/Pub Message with the info for updating the hostess salary.

    Returns:
        http_status: Corresponding status code (200 for success and 400 for failure).
    """

    if request.method == "GET":
        return "<h1> This is a webhook listener!</h1>"
    if request.method == "POST":
        try:
            posted_data = request.json
            print("We have received a request")
            print(posted_data)

            hostess_name = posted_data["names"]
            type_request = posted_data["type"]
            attempt_number = posted_data["attempt"]
            if type_request == "test":
                print("Test")
                return "<h1>Test run correctly</h1>"
            month = posted_data["month"]
            year = posted_data["year"]

            if type_request == "regular":
                lis_names = process_sheets_from_master(month, year, hostess_name)
            elif type_request == "retry":
                print(f"trying retry for: {hostess_name}")
                lis_names = process_sheets_from_master(
                    month, year, hostess_name, attempts=attempt_number
                )
            if len(lis_names) > 0:
                print(f"Retrying failed attempts for: {','.join(lis_names)}")
                process_sheets_from_master(month, year, lis_names, attempts = 2)
            elif len(lis_names) == 0:
                print("No retries left, finishing process")

        except Exception as e:
            print("Error in process_sheet:",e)
            http_status = "", 400
        http_status = jsonify({"status": "success"}), 200
    else:
        http_status = "", 400
    return http_status
