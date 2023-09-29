## First we get the data from the master spreadsheet
## Second we filter the important data
## Third we loop through the hostess names and update each file and spreadsheet
## Fourth we update the master spreadsheet ---- OPTIONAL
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
        n_col = worksheet.get_values("N2:N")

        hostes_dict = {A[0]:N[0] for A,N in zip(a_col,n_col)}
    except Exception as e:
        print(e)
        return False
    return hostes_dict
