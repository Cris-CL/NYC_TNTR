import base64
import functions_framework
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import pytz

URL = os.environ.get("URL")

def create_message(month_select, test_var=True):
    """
    This function takes 2 parameters: month_select a number between 1 and 12
    and test_var a boolean, it creates the json message that will be send to
    update the Worksheets of every hostess, if the test_var parameter is the
    default (True), then the message will be a test one and it will not trigger
    the later function in the URL.

    Parameters:

    - month_select (int): Integer indicating which month of the current year will
    be processed.

    - test_var (bool): Boolean to indicate if the run will be a test or not.

    Returns:

     - data_send (dict): Dictionary encapsullating all the necessary data for
     the funcion in the URL to work.
    """
    timezone_name = "Asia/Tokyo"

    if test_var == True:
        name_var = "Test"
        type_request = "test"
    if test_var == False:
        name_var = "All"
        type_request = "regular"
        current_hour = datetime.now(pytz.timezone(timezone_name)).hour
        print(f"Sending update request at {current_hour}")

    dt = datetime.now(pytz.timezone(timezone_name))

    if month_select == 1:
        month = dt.month
    elif month_select == 2:
        dt = dt - relativedelta(months=1)
        month = dt.month
    else:
        print(f"Wrong message {month_select}")
        return
    year = dt.year

    data_send = {"type": type_request, "names": name_var, "month": month, "year": year}
    return data_send

def send_request(message):
    """
    This function sends a POST request to the URL with the message dict given as
    a parameter.

    Parameters:

    - message (dict): The message parameter containing all the information for the function
    in the URL, this message is obtained by the create_message funtion.
    """
    headers = {"Content-Type": "application/json"}
    print(message)
    try:
        requests.post(URL, json=message, headers=headers, timeout=5)
    except Exception as e:
        print(e)
        print(f"POST request Send to {URL}")
    return

@functions_framework.cloud_event
def send_hss_update(cloud_event):
    """
    This function is triggered by a cloud scheduler job that runs every day,
    2 times, one for the previous month and one for the current month. in the
    cloud event that is a PUB/SUB message, that contains the month_select
    information used to determine if the month to process is the current or the
    previous one, once determined that, the message to send is build with the
    function create_message, and then send with the function send_request.

    Parameters:

    - cloud_event: event generated when a PUB/SUB message is published
    """
    month_sel = cloud_event.data["message"]["attributes"]["month_select"]
    if "previous" in month_sel:
        data_message = create_message(month_select=2, test_var=False)
    elif "current" in month_sel:
        data_message = create_message(month_select=1, test_var=False)
    else:
        print("something wrong")

    send_request(data_message)
    print(cloud_event.data["message"])
    return
