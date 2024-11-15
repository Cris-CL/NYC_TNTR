import gspread
import google.auth
from time import sleep


def get_hostess_dict_0(master_id):
    try:
        try:
            gc = gspread.service_account()
        except:
            credentials, _ = google.auth.default()
            gc = gspread.authorize(credentials)

        nyc_master_hostess_data = gc.open_by_key(master_id)
        worksheet_name = "MASTER"  #### Maybe this should be an environment variable
        worksheet = nyc_master_hostess_data.worksheet(worksheet_name)

        a_col = worksheet.get_values("A2:A")
        p_col = worksheet.get_values("P2:P")

        hostes_dict = {A[0]:P[0] for A,P in zip(a_col,p_col)}
    except Exception as e:
        print("Error in get_hostess_dict",e)
        return False
    return hostes_dict


def get_hostess_dict(master_id):
    sleep_time = 31
    retry_count = 0
    try:
        gc = gspread.service_account()
    except:
        credentials, _ = google.auth.default()
        gc = gspread.authorize(credentials)
    while True:
        try:
            nyc_master_hostess_data = gc.open_by_key(master_id)
            worksheet_name = "MASTER"  #### Maybe this should be an environment variable
            worksheet = nyc_master_hostess_data.worksheet(worksheet_name)

            a_col = worksheet.get_values("A2:A")
            p_col = worksheet.get_values("P2:P")

            hostes_dict = {A[0]:P[0] for A,P in zip(a_col,p_col)}
            break
        except Exception as e:
            print('Error in get_hostess_dict',e)
            if retry_count > 0:
                retry_count += 1
                print('Reached max retries for get_hostess_dict')
                return False
            else:
                print("Retrying get_hostess_dict after 30 seconds wait")
                retry_count += 1
                sleep(sleep_time)
                continue
    return hostes_dict
