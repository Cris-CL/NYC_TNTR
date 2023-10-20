import gspread
import google.auth


def get_hostess_dict(master_id):
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
        print(e)
        return False
    return hostes_dict
