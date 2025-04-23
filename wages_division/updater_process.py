import os
import pandas as pd
import google.auth
import gspread
from time import sleep
from sheets_util import (
    days_in_month,
    calc_gensen,
    col_to_number,
    format_worksheet,
    resize_columns,
    clean_cell,
    clear_formatting,
)
from handlers import handle_other_errors, handle_rate_limit, handle_gspread_error
import datetime


def str_to_float(x):
    try:
        x = float(x.replace("¥", "").replace(",", ""))
    except:
        x = 0
    return x


def reverse_list_odd_date(lst):
    current_date = datetime.datetime.now().day

    if current_date % 2 == 0:
        result_list = lst
    else:
        result_list = list(reversed(lst))
    return result_list


def calc_totals_nrws(worksheet, year, month):
    date_str = f"{year}{month}"
    days_in_mon = days_in_month(date_str)
    try_number = 0
    while True:
        try:
            # Get all values in the worksheet
            values = worksheet.get_all_values()
            break
        except Exception as e:
            hanler = handle_gspread_error(e, "calc_totals_nrws part_1", "nenen")
            if hanler == True and try_number == 0:
                try_number = try_number + 1
                continue
            else:
                raise e
    # Add two empty rows at the bottom
    num_columns = len(values[0])
    for _ in range(2):
        values.append([""] * num_columns)

    # Calculate and set totals in the third row
    start_column = 5  # Column E
    end_column = 29  # Column AC

    subtotal_row = [""] * num_columns
    discounts_row = [""] * num_columns
    grand_total_row = [""] * num_columns

    gensen_row = [""] * num_columns
    paid_amount_row = [""] * num_columns

    for col in range(start_column, end_column + 1):
        try:
            column_values = []
            for row in values:
                cell_value = row[col - 1]
                if cell_value.strip().replace(".", "", 1).isdigit():
                    column_values.append(float(cell_value))
                else:
                    # Convert yen currency-formatted values to float
                    try:
                        currency_value = float(
                            cell_value.strip("¥").replace(",", "").replace("¥", "")
                        )
                        column_values.append(currency_value)
                    except ValueError:
                        pass

            column_sum = sum(column_values)
            subtotal_row[col - 1] = column_sum  # Adjust for 0-based index
        except ValueError:
            pass
    # Add "GRAND TOTAL" in column A

    # Calculate and add the sum of columns F and AB in column B
    column_F_values = [str_to_float(row[5]) for row in values]
    column_Z_values = [str_to_float(row[25]) for row in values]

    column_AA_values = [str_to_float(row[26]) for row in values]
    column_AB_values = [str_to_float(row[27]) for row in values]
    column_AC_values = [str_to_float(row[28]) for row in values]

    subtotal = sum(column_F_values) + sum(column_Z_values)

    subtotal_row[0] = "SUBTOTAL"
    subtotal_row[1] = subtotal

    discounts_row[0] = "OTHERS"
    discounts = sum(column_AA_values) + sum(column_AB_values) + sum(column_AC_values)
    discounts_row[1] = discounts

    gensen_row[0] = "源泉徴収税"
    gensen = calc_gensen(subtotal, days_in_mon)
    gensen_row[1] = gensen

    paid_amount_row[0] = "支払金額合計"
    paid_amount_row[1] = subtotal + gensen

    grand_total_row[0] = "GRAND TOTAL"
    grand_total_row[1] = subtotal + discounts + gensen

    # Append the totals row to the worksheet
    values.append(subtotal_row)
    values.append(gensen_row)
    values.append(paid_amount_row)
    values.append(discounts_row)
    values.append(grand_total_row)

    try_number = 0
    # Update the worksheet with the modified values
    while True:
        try:
            worksheet.update(values, value_input_option="USER_ENTERED")
            return True
        except Exception as e:
            hanler = handle_gspread_error(e, "calc_totals_nrws part_2", "nenen")
            if hanler == True and try_number == 0:
                try_number = try_number + 1
                continue
            else:
                print(
                    f"An error occurred in calc_totals_nrws for file {worksheet.spreadsheet.title} type: {type(e)}",
                )
                raise e
    return False


def get_spreadsheet(spreadsheet_name=None, spreadsheet_id=None):
    # Specify the Google Sheets document and worksheet

    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )

    gc = gspread.authorize(credentials)

    # Open the Google Sheets document
    try:
        if spreadsheet_id:
            sh = gc.open_by_key(spreadsheet_id)
        else:
            sh = gc.open(spreadsheet_name)
        return sh
    except Exception as e:
        print(f"Error in get_spreadsheet with id= {spreadsheet_id}")
        raise e


def get_specific_hostess_df(df, hostess_name):
    loc_df = df.copy()
    columns_ind = [
        "DAY",
        "cp_code",
        "starting_time",
        "leaving_time",
        "worked_hours",
        "wage_total_daily",
        "BT",
        "DR1",
        "DR2",
        "DR3",
        "TP",
        "DH",
        "HC",
        "MR",
        "HR",
        "X",
        "TOTAL_NOT_T",
        "BT_T",
        "DR1_T",
        "DR2_T",
        "DR3_T",
        "TP_T",
        "KA_T",
        "X395",
        "TOTAL_T",
        "TOTAL_DAY",
        "送り",
        "DISC",
        "ADV",
        "notes",
    ]
    new_names = {
        "wage_total_daily": "SUBTOTAL_WAGE",
        "TOTAL_NOT_T": "TOTAL_UC",
        "X": "EXTRA",
        "X395": "EXTRA_395",
        "TOTAL_T": "SUBTOTAL_395",
    }
    try:
        loc_df = loc_df[loc_df["hostess_name"] == hostess_name][columns_ind].copy()

        loc_df.columns = [new_names.get(col_old, col_old) for col_old in loc_df.columns]

        loc_df.columns = [col.replace("_T", "_395") for col in loc_df.columns]
    except Exception as e:
        print(f"Error in get_specific_hostess_df for {hostess_name}")
        raise e
    return loc_df


def prepare_hostess_list(sh_hostess_dict):
    """
    Prepares the list of hostesses to be processed.

    Args:
        sh_hostess_dict (dict): A dictionary mapping hostess names to their spreadsheet IDs.

    Returns:
        list: A list of hostess names, with "店" removed if present.
    """
    list_hostess = list(sh_hostess_dict.keys())
    list_hostess = reverse_list_odd_date(list_hostess)
    print(f"Processing all {len(list_hostess)} hostess")

    try:
        list_hostess.remove("店")
    except ValueError:
        # print("店 not in the file")
        pass ### added instead of previous pring

    return list_hostess


def process_hostess(name, results_df, sh_hostess_dict, year, month):
    """
    Processes a single hostess by updating her spreadsheet with the latest data.

    Args:
        name (str): The name of the hostess.
        results_df (DataFrame): The results DataFrame containing data for all hostesses.
        sh_hostess_dict (dict): A dictionary mapping hostess names to their spreadsheet IDs.
        year (int): The year to be used for naming new sheets.
        month (int): The month to be used for naming new sheets.

    Returns:
        bool: True if the hostess's spreadsheet was successfully updated, False otherwise.
    """
    waiting_time = 10
    while True:
        try:
            id_sh = sh_hostess_dict.get(name)
            if not id_sh:
                print(f"Hostess {name} not found in the master spreadsheet")
                return False

            sh = get_spreadsheet(spreadsheet_id=id_sh)
            if not validate_spreadsheet_name(sh, name):
                return False

            df_orig_temp = get_specific_hostess_df(results_df, name)
            df_temp = df_orig_temp.copy()
            if df_temp.empty:
                print(f"No data for {name}")
                return False
            try:
                df_temp["net_day"] = (
                    col_to_number(df_temp["SUBTOTAL_WAGE"])
                    + col_to_number(df_temp["TOTAL_DAY"])
                    + col_to_number(df_temp["送り"])
                    + col_to_number(df_temp["DISC"])
                    + col_to_number(df_temp["ADV"])
                )
            except Exception as e:
                print(
                    f"Error in col_to_number for {name} message: {e}, type: {type(e)}"
                )
                df_temp = pd.DataFrame()
                print("Using original dataframe without net_day column")
                df_temp = df_orig_temp

            active_worksheet = prepare_worksheet(sh, df_temp, year, month)

            update_worksheet(active_worksheet, df_temp)
            calc_totals_nrws(active_worksheet, year, month)

            try:
                format_worksheet(active_worksheet)
            except Exception as e:
                print(
                    f"Error in process_hostess format_worksheet step for {name} Sheet message: {e}, type: {type(e)}"
                )
            try:
                resize_columns(FILE=sh, sheet_name=active_worksheet.title)
            except Exception as e:
                print(
                    f"Error in process_hostess rezise_columns step for {name} Sheet message: {e}, type: {type(e)}"
                )
            sleep(6)
            return True

        except Exception as e:
            if "RATE_LIMIT_EXCEEDED" in str(e):
                waiting_time = handle_rate_limit(waiting_time, name)
                sleep(waiting_time)
            else:
                handle_other_errors(name, e)
                return False


def validate_spreadsheet_name(sh, name):
    """
    Validates that the spreadsheet name matches the hostess name.

    Args:
        sh (Spreadsheet): The Google Spreadsheet object.
        name (str): The name of the hostess.

    Returns:
        bool: True if the spreadsheet name matches the hostess name, False otherwise.
    """
    spreadsheet_name = sh.title
    if name.lower() not in spreadsheet_name.lower():
        print(
            f"Spreadsheet: {spreadsheet_name} doesn't match with hostess name: {name}"
        )
        return False
    return True


def prepare_worksheet(sh, df_temp, year, month):
    """
    Prepares the worksheet for updating by clearing it or adding a new one if necessary.

    Args:
        sh (Spreadsheet): The Google Spreadsheet object.
        df_temp (DataFrame): The DataFrame with the hostess data to be updated.
        year (int): The year to be used for naming new sheets.
        month (int): The month to be used for naming new sheets.

    Returns:
        Worksheet: The active worksheet ready for updates.
    """
    try:
        sheets = [sht_name.title for sht_name in sh.worksheets()]
        month_str = str(month).zfill(2)
        new_sheet_name = f"{year}{month_str}"

        if new_sheet_name not in sheets:
            active_worksheet = sh.add_worksheet(
                title=new_sheet_name,
                rows=str(df_temp.shape[0]),
                cols=str(df_temp.shape[1]),
            )
        else:
            active_worksheet = sh.worksheet(new_sheet_name)
            clear_formatting(FILE=sh, sheet_name=new_sheet_name)
    except Exception as e:
        print(f"Error in prepare_worksheet for sheet {sh.title} {year} {month}")
        raise e
    active_worksheet.clear()
    return active_worksheet


def update_worksheet(active_worksheet, df_temp):
    """
    Updates the worksheet with the given DataFrame data.

    Args:
        active_worksheet (Worksheet): The worksheet to be updated.
        df_temp (DataFrame): The DataFrame with the hostess data to be updated.
    """
    amount_try = 0
    while True:
        try:
            cell_list = active_worksheet.range(
                1, 1, len(df_temp) + 1, len(df_temp.columns)
            )

            for cell in cell_list:
                if cell.row == 1:
                    cell.value = df_temp.columns[cell.col - 1]
                else:
                    cell.value = str(df_temp.iloc[cell.row - 2, cell.col - 1])

            cell_list = [clean_cell(cell_dirty) for cell_dirty in cell_list]
            active_worksheet.resize(
                rows=str(df_temp.shape[0]), cols=str(df_temp.shape[1])
            )
            active_worksheet.update_cells(cell_list, value_input_option="USER_ENTERED")
            return True
        except Exception as e:
            if "JSONDecodeError" in str(e) and amount_try == 0:
                print(
                    f"Error in update_worksheet for {active_worksheet.spreadsheet.title}"
                )
                raise e
            else:
                amount_try = amount_try + 1
                sleep(5)
                continue
            raise e


def updater(results_df, sh_hostess_dict, year, month):
    """
    Updates all hostess spreadsheets with the given results data.

    Args:
        results_df (DataFrame): The results DataFrame containing data for all hostesses.
        sh_hostess_dict (dict): A dictionary mapping hostess names to their spreadsheet IDs.
        year (int): The year to be used for naming new sheets.
        month (int): The month to be used for naming new sheets.

    Returns:
        list: A list of hostess names that could not be updated.
    """
    list_hostess = prepare_hostess_list(sh_hostess_dict)
    names_not_updated = []

    for name in list_hostess:
        if not process_hostess(name, results_df, sh_hostess_dict, year, month):
            names_not_updated.append(name)
        if len(names_not_updated) > 15:
            print("Too many errors, aborting operation")
            return []
    if names_not_updated:
        print(f"Couldn't update the following people: {', '.join(names_not_updated)}")

    return names_not_updated
