import gspread
import google.auth
from time import sleep
import calendar
from handlers import handle_gspread_error


def clear_formatting(FILE, sheet_name):
    """
    This function clears the format for the worksheet FILE in the sheet_name.

    Args:
        FILE (gspread.file): Gspread Worksheet.
        sheet_name (gspreadheet.sheet): Gspread Sheet.
    """
    try_number = 0
    while True:
        try:
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
            break
        except Exception as e:
            handler = handle_gspread_error(e, "clear_formatting part_1", "nonoe")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
            else:
                raise e
    try_number = 0
    while True:
        try:
            FILE.batch_update(body)
            break
        except Exception as e:
            handler = handle_gspread_error(e, "clear_formatting part_2", "nonoe")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
            else:
                print(
                    f"Error in clear_formatting on file: {FILE.title} and sheet: {sheet_name} type: {type(e)}"
                )
    return


def format_worksheet(worksheet):
    """
    This function gives the final formatting for the worksheet so all the columns
    get the proper format.

    Args:
        worksheet (int): worksheet to be formatted.
    """
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
        "AE": "currency",
    }
    num_rows = len(worksheet.get_all_values())
    if int(worksheet.col_count) < 31:
        col_types.pop("AE")

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
    try_number = 0
    while True:
        try:
            worksheet.batch_format(batch)
            break
        except Exception as e:
            handler = handle_gspread_error(e, "format_worksheet", "none")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
            elif "exhausted" in str(e).lower() or "exeeded" in str(e).lower():
                print(
                    f"Waiting for {format_waiting} seconds before retrying format_worksheet"
                )
                sleep(format_waiting)
                format_waiting = format_waiting + 1
            else:
                print(
                    f"Error in format_worksheet for {worksheet.title} and type: {type(e)}"
                )
                raise e
    return


def resize_columns(FILE, sheet_name):
    """
    This function resize the columns for the FILE and sheet_name.

    Args:
        FILE (gspread.file): Gspread Worksheet.
        sheet_name (gspreadheet.sheet): Gspread Sheet.
    """
    try_number = 0
    while True:
        try:
            wsht = FILE.worksheet(sheet_name)
            break
        except Exception as e:
            handler = handle_gspread_error(e, "resize_columns part_1", "none")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
    sheetId = int(wsht._properties["sheetId"])
    try:
        END_INDEX = wsht.col_count
    except:
        END_INDEX = 30
    body = {
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheetId,
                        "dimension": "COLUMNS",
                        "startIndex": 1,
                        "endIndex": END_INDEX,
                    }
                }
            }
        ]
    }
    try_number = 0
    while True:
        try:
            FILE.batch_update(body)
            return
        except Exception as e:
            handler = handle_gspread_error(e, "resize_columns part_2", "none")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
            elif "exhausted" in str(e).lower() or "exceeded" in str(e).lower():
                print(
                    f"API rate limit exceeded. Waiting 5 and retrying formatting {sheet_name} sheet"
                )
                sleep(5)
                continue
            else:
                file_name = int(wsht._properties)
                print(
                    f"resize_columns: Couldnt resize the sheet {sheet_name} on {file_name} message: {e} and type: {type(e)}"
                )
                return


def clean_cell(unit_cell):
    """
    This function cleans the numeric cells and replaces the common nan/null values
    for emtpy strings where is the case.

    Args:
        unit_cell (str): single cell raw.
    Returns:
        unit_cell (float): single cell cleaned.
    """

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
    """
    This function calculates the amount of days in a specific month to be used
    to calculate the gensen.

    Args:
        date_string (str): Date in yyyymmdd format.
    Returns:
        num_days (int): Number of days on the specific month.
    """
    try:
        year = int(date_string[0:4])
        month = int(date_string[4:6])
    except ValueError:
        return "Invalid date format. Please use 'yyyymmdd' format."

    _, num_days = calendar.monthrange(year, month)
    return num_days


def calc_gensen(subtotal, days_in_month):
    """
    Calculates the 源泉徴収税 (gensen) for the current hostess, the formula is the subtotal minus 5,000 yen times the
    amount of days in the current month, if that value is more than 0 then multiply it by 0.1021, round it and make
    it negative, in case the value is less than 0 then returns 0.

    Args:
        subtotal (int): The wage plus commission earned in the month.
        days_in_month (int): The amount of days the current month has.

    Returns:
        int: The negative and rounded gensen.
    """
    gensen = (subtotal - 5000 * days_in_month) * 0.1021
    if gensen > 0:
        return round(-gensen)
    else:
        return 0


def col_to_number(col_df):
    """
    This function transforms and cleans a numeric column from a pandas dataframe.

    Args:
        col_df (df.column): Dataframe column.
    Returns:
        new_col (df.column): cleaned column.
    """
    new_col = col_df.copy().map(
        lambda x: float(x) if isinstance(x, str) and x[-1].isnumeric() else 0
    )
    return new_col
