import gspread
import google.auth
from time import sleep
import calendar


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
        print('Error get_hostess_dict')
        print(e,type(e))
        return False
    return hostes_dict


def clear_formatting(FILE, sheet_name):
    wsht = FILE.worksheet(sheet_name)
    sheetId = int(wsht._properties["sheetId"])
    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {"sheetId": sheetId},
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat",
                    }
             }
            ]
        }
    try:
        FILE.batch_update(body)
    except Exception as e:
        print(f"Error in clear_formatting on file: {FILE.title} and sheet: {sheet_name}",e,type(e))
        return


def format_worksheet(worksheet):
    format_waiting = 5

    cell_types = {
        "number": {"numberFormat": {"type": "NUMBER", "pattern": "0"}},
        "number_decimal": {"numberFormat": {"type": "NUMBER", "pattern": "#0.#0"}},
        "time": {"numberFormat": {"type": "TIME", "pattern": "hh:mm:ss am/pm"}},
        "currency": {"numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}},
    }

    col_types = {
        "C": "time",
        "D": "time",
        "E": "number_decimal",
        "F": "currency",
        "G": "currency",
        "H": "currency",
        "I": "currency",
        "J": "currency",
        "K": "currency",
        "L": "currency",
        "M": "currency",
        "N": "currency",
        "O": "currency",
        "P": "currency",
        "Q": "currency",
        "R": "currency",
        "S": "currency",
        "T": "currency",
        "U": "currency",
        "V": "currency",
        "W": "currency",
        "X": "currency",
        "Y": "currency",
        "Z": "currency",
        "AA": "currency",
        "AB": "currency",
        "AC": "currency",
        "AD": "number",
    }
    num_rows = len(worksheet.get_all_values())
    format_options = {
        col: cell_types.get(col_types.get(col, "number"), cell_types["number"])
        for col in col_types.keys()
    }
    batch = [
        {
            "range": f"{col}2:{col}",
            "format": options,
            "valueInputOption": "USER_ENTERED",
        }
        for col, options in format_options.items()
    ]
    batch_2 = [
        {
            "range": f"B{num_rows - 4}",
            "format": cell_types["currency"],
            "valueInputOption": "USER_ENTERED",
        },
        {
            "range": f"B{num_rows - 3}",
            "format": cell_types["currency"],
            "valueInputOption": "USER_ENTERED",
        },
        {
            "range": f"B{num_rows - 2}",
            "format": cell_types["currency"],
            "valueInputOption": "USER_ENTERED",
        },
        {
            "range": f"B{num_rows - 1}",
            "format": cell_types["currency"],
            "valueInputOption": "USER_ENTERED",
        },
        {
            "range": f"B{num_rows}",
            "format": cell_types["currency"],
            "valueInputOption": "USER_ENTERED",
        },
    ]
    batch = batch + batch_2
    while True:
        try:
            worksheet.batch_format(batch)
            break
        except Exception as e:
            if "exhausted" in str(e).lower() or "exeeded" in str(e).lower():
                print(
                    f"Waiting for {format_waiting} seconds before retrying format_worksheet"
                )
                print(e,type(e))
                sleep(format_waiting)
                format_waiting = format_waiting + 1
            else:
                print("problem with formatting")
                print(e,type(e))
                raise
    return


def resize_columns(FILE, sheet_name):
    wsht = FILE.worksheet(sheet_name)
    sheetId = int(wsht._properties["sheetId"])
    body = {
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheetId,
                        "dimension": "COLUMNS",
                        "startIndex": 1,
                        "endIndex": 30,
                    }
                }
            }
        ]
    }
    while True:
        try:
            FILE.batch_update(body)
            return
        except Exception as e:
            if "exhausted" in str(e).lower() or "exceeded" in str(e).lower():
                print(
                    f"API rate limit exceeded. Waiting 5 and retrying formatting {sheet_name} sheet"
                )
                sleep(5)
            else:
                print(e,type(e))
                file_name = int(wsht._properties)
                print(f"Couldnt resize the sheet {sheet_name} on {file_name}")
                return


def clean_cell(unit_cell):
    if unit_cell.value.lower() in ["nan", "none", "nat", "null", "<na>", " "]:
        unit_cell.value = ""
    else:
        pass
    try:
        unit_cell.value = float(unit_cell.value)
    except:
        pass
    return unit_cell


def days_in_month(date_string):
    try:
        year = int(date_string[0:4])
        month = int(date_string[4:6])
    except ValueError:
        return "Invalid date format. Please use 'yyyymmdd' format."

    _, num_days = calendar.monthrange(year, month)
    return num_days


def calc_gensen(subtotal, days_in_month):

    if (subtotal - 5000 * days_in_month) * 0.1021 > 0:
        return round(-(subtotal - 5000 * days_in_month) * 0.1021)
    else:
        return 0
