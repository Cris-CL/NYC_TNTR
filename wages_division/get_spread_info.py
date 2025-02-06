import gspread
import google.auth
from time import sleep
from sheets_util import handle_gspread_error


def get_hostess_dict(master_id):
    try:
        gc = gspread.service_account()
    except:
        credentials, _ = google.auth.default()
        gc = gspread.authorize(credentials)
    try_number = 0
    while True:
        try:
            nyc_master_hostess_data = gc.open_by_key(master_id)
            worksheet_name = "MASTER"  #### Maybe this should be an environment variable
            worksheet = nyc_master_hostess_data.worksheet(worksheet_name)

            a_col = worksheet.get_values("A2:A")
            p_col = worksheet.get_values("P2:P")

            hss_dict = {A[0]: P[0] for A, P in zip(a_col, p_col)}
            break
        except Exception as e:
            handler = handle_gspread_error(e, "get_hostess_dict", "nonoe")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
            else:
                print("Error in get_hostess_dict", e, type(e))
                return {}
    return hss_dict
