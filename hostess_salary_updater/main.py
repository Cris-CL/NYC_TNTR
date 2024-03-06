import base64
import functions_framework
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import pytz

URL = os.environ.get("URL")

def send_request(month_select,test_var=True):
    if test_var == True:
        name_var = 'Test'
    if test_var == False:
        name_var = 'All'
        current_hour = datetime.utcnow().hour+9
        print(f"Sending update request at {current_hour}")
    timezone_name = 'Asia/Tokyo'
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
        "name": name_var,
        "month": month,
        "year":year
        }
    headers = {'Content-Type': 'application/json'}
    print(data_send)
    try:
      requests.post(URL,json=data_send,headers=headers,timeout=5)

    # except requests.exceptions.ReadTimeout:
    #     print(f"POST request Send to {URL}")
    except Exception as e:
        print(e)
        print(f"POST request Send to {URL}")
    return

@functions_framework.cloud_event
def updater(cloud_event):
    month_sel = cloud_event.data["message"]['attributes']['month_select']
    if 'previous' in month_sel:
        send_request(month_select=2,test_var=False)
    elif 'current' in month_sel:
        send_request(month_select=1,test_var=False)
    else:
        print("something wrong")
    print(cloud_event.data["message"])
    return
