import base64
import functions_framework
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import pytz

URL = os.environ.get("URL")

def create_message(month_select,test_var=True):
    """
    TODO docstrings
    """
    timezone_name = 'Asia/Tokyo'

    if test_var == True:
        name_var = 'Test'
        type_request = 'test'
    if test_var == False:
        name_var = 'All'
        type_request = 'regular'
        current_hour = datetime.now(pytz.timezone(timezone_name)).hour
        print(f"Sending update request at {current_hour}")

    dt = datetime.now(pytz.timezone(timezone_name))

    if month_select ==1:
        month = dt.month
    elif month_select ==2:
        dt = dt - relativedelta(months=1)
        month = dt.month
    else:
        print(f"Wrong message {month_select}")
        return
    year = dt.year

    data_send = {
        "type": type_request,
        "names": name_var,
        "month": month,
        "year":year
        }
    return data_send


def send_request(message):
    """
    TODO docstrings
    """
    headers = {'Content-Type': 'application/json'}
    print(message)
    try:
      requests.post(URL,json=message,headers=headers,timeout=5)
    except Exception as e:
        print(e)
        print(f"POST request Send to {URL}")
    return

@functions_framework.cloud_event
def send_hss_update(cloud_event):
    """
    This function sends a POST request to the URL with the month and year as the payload.
    the trigger for this function is a cloud scheduler job that runs every day, 2 times,
    one for the previous month and one for the current month.
    """
    month_sel = cloud_event.data["message"]['attributes']['month_select']
    if 'previous' in month_sel:
        data_message = create_message(month_select=2,test_var=False)
    elif 'current' in month_sel:
        data_message = create_message(month_select=1,test_var=False)
    else:
        print("something wrong")

    send_request(data_message)
    print(cloud_event.data["message"])
    return
