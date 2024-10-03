import os
import gspread
import pandas as pd
import google.auth
import calendar
from time import sleep
from google.cloud import bigquery

# from new_query import create_new_query


def create_new_query(month, year):
    ## Add a zero if the month is less than 10
    month_str = (2 - len(str(month))) * "0" + str(month)
    query = f"SELECT * from tantra.HostessWagesForPaySlip('{year}','{month_str}')"
    return query


def get_df_full(month, year, cred):
    PROJECT_ID = os.environ["PROJECT_ID"]
    print(PROJECT_ID)
    query = create_new_query(month, year)
    df_query = pd.read_gbq(query, project_id=PROJECT_ID, credentials=cred)
    return df_query.copy()


def get_bill_file(bill_file_id, credentials=None):
    # bill_file_id = os.environ["OID"]
    print(bill_file_id)
    # credentials, _ = google.auth.default()
    gc = gspread.authorize(credentials)
    bill_file = gc.open_by_key(bill_file_id)
    return bill_file


def calc_gensen(subtotal, days_in_month):
    if (subtotal - 5000 * days_in_month) * 0.1021 > 0:
        return round(-(subtotal - 5000 * days_in_month) * 0.1021)
    else:
        return 0


def days_in_month(date_string):
    try:
        year = int(date_string[0:4])
        month = int(date_string[4:6])
    except ValueError:
        return "Invalid date format. Please use 'yyyymmdd' format."
    _, num_days = calendar.monthrange(year, month)
    return num_days


def get_bill_master_df(df_query):

    df_query = df_query[
        [
            "DAY",
            "hostess_name",
            "cp_code",
            "worked_hours",
            "wage_total_daily",
            "TOTAL_DAY",
            "X395",
            "X",
            "送り",
            "DISC",
            "ADV",
            "amount_tips",
        ]
    ].copy()
    df_query["DATE"] = pd.to_datetime(df_query["DAY"], format="%Y%m%d")

    return df_query.copy()


def filter_bill(hostess_name, bill_master):
    df_temp = bill_master.copy()
    df_temp = df_temp[df_temp["hostess_name"] == hostess_name]
    return df_temp.copy()


def turn_number(number):
    try:
        return float(number)
    except:
        # print(f"Error converting {number} to float.")
        return 0.0


def copy_template(FILE, new_name, template_name):

    template = FILE
    example_sheet = template.worksheet(template_name)
    list_sheets = [sheets.title for sheets in FILE.worksheets()]

    if new_name in list_sheets:
        return template.worksheet(new_name)

    # return example_sheet
    if example_sheet:
        # Make a copy of the 'Example' sheet with the new name
        new_example_sheet = example_sheet.duplicate(new_sheet_name=new_name)
    else:
        raise ValueError("Sheet 'Example' not found in the template.")

    return new_example_sheet


def add_new_rows_to_worksheet(
    worksheet, wage_comission_rows, discount_rows, cp_code, month_str="20230901"
):
    # Get the worksheet title and row data
    # worksheet_title = worksheet.title

    current_rows = worksheet.get_all_values()
    subtotal = 0
    disc_subtotal = 0
    # return current_rows
    for row_new in wage_comission_rows:
        subtotal = subtotal + turn_number(row_new[3])
    for dis_row in discount_rows:
        disc_subtotal = disc_subtotal + turn_number(dis_row[3])
    print(f"subtotal: {subtotal} disc_subtotal: {disc_subtotal}")
    if subtotal != 0:
        ten_percent = round(-(subtotal / 11))
    else:
        ten_percent = 0

    days = days_in_month(month_str)
    ten_21_percent = calc_gensen(subtotal, days)

    total_payment_amount = round(subtotal + ten_21_percent)

    if disc_subtotal != 0:
        disc_ten_percent = round(-(disc_subtotal / 11))
    else:
        disc_ten_percent = 0

    total = total_payment_amount + disc_subtotal
    current_rows[2][0] = '=VLOOKUP(B5,MASTER_MHS!$A:$C,3,false)&" 様"'
    current_rows[3][1] = "=VLOOKUP(B5,MASTER_MHS!$A:$B,2,false)"
    current_rows[4][1] = cp_code
    current_rows[7][1] = total_payment_amount  # 支払金額合計
    current_rows[8][1] = ten_percent  # 消費税（10％）
    current_rows[16][3] = subtotal  # 小計
    current_rows[17][3] = ten_21_percent  # 源泉徴収税額

    new_added = current_rows[:15] + wage_comission_rows + current_rows[15:]

    # Insert the new rows after the 15th row
    worksheet.update(new_added, value_input_option="USER_ENTERED")

    return worksheet


def process_discounts(FILE, name, discount_rows, cp_code):
    disc_subtotal = 0

    for dis_row in discount_rows:
        disc_subtotal = disc_subtotal + turn_number(dis_row[3])
    if disc_subtotal == 0:
        print(disc_subtotal)
        print(f"No discounts to process for {name}")
        return

    if disc_subtotal != 0:
        disc_ten_percent = round((disc_subtotal / 11))
    else:
        disc_ten_percent = 0

    new_sheet_name = f"{name}_2"
    disc_wsh = copy_template(FILE, new_sheet_name, "Example_2")
    current_rows = disc_wsh.get_all_values()

    current_rows[4][1] = cp_code
    current_rows[2][0] = '=VLOOKUP(B5,MASTER_MHS!$A:$C,3,false)&" 様"'
    current_rows[3][1] = "=VLOOKUP(B5,MASTER_MHS!$A:$B,2,false)"

    current_rows[6][1] = abs(disc_subtotal)
    current_rows[7][1] = disc_ten_percent
    new_added = current_rows[:10] + discount_rows + current_rows[12:]

    # Insert the new rows after the 15th row
    disc_wsh.update(new_added, value_input_option="USER_ENTERED")

    return


def get_wage_comission(dataframe):
    rows = []

    for _, row in dataframe.iterrows():

        date = row["DATE"]
        wage_total_daily = turn_number(row["wage_total_daily"])
        total_day = turn_number(row["TOTAL_DAY"])
        X395 = turn_number(row["X395"])
        X = turn_number(row["X"])
        extra_comission = 0
        if (isinstance(X395, float) or isinstance(X395, int)) and X395 > 0:
            extra_comission = extra_comission + X395

        if (isinstance(X, float) or isinstance(X, int)) and X > 0:
            extra_comission = extra_comission + X

        total_day = round(total_day)
        # total_day = total_day + extra_comission
        if wage_total_daily > 0:
            rows.append(
                [
                    date.strftime("%Y-%m-%d"),
                    "Show",
                    wage_total_daily,
                    wage_total_daily,
                    "",
                    "",
                    "",
                ]
            )
        if total_day > 0:
            rows.append(
                [
                    date.strftime("%Y-%m-%d"),
                    "Commission",
                    total_day,
                    total_day,
                    "",
                    "",
                    "",
                ]
            )

    rows.sort(key=lambda x: x[0])

    return rows


def get_discounts(dataframe):
    rows = []
    other_discounts = 0
    taxi = 0
    count_taxi = 0
    for _, row in dataframe.iterrows():
        okuri = turn_number(row["送り"])
        if okuri != 0:
            count_taxi = count_taxi + 1
        DISC = turn_number(row["DISC"])
        ADV = turn_number(row["ADV"])
        X395 = turn_number(row["X395"])
        X = turn_number(row["X"])

        if X395 < 0:
            other_discounts = other_discounts + X395
        if X < 0:
            other_discounts = other_discounts + X
        taxi = taxi + okuri
        other_discounts = other_discounts + DISC + ADV
    if count_taxi == 0:
        avg_taxi = 0
    else:
        avg_taxi = taxi // count_taxi

    rows.append(["タクシー利用料", count_taxi, avg_taxi, taxi, "", ""])
    rows.append(["その他", "", "", other_discounts, "", "", ""])

    return rows


def get_all_bills(bill_master_df, cred, month_string, bill_id):
    total_errors = 0
    waiting_time = 20
    BILL_FILE = get_bill_file(bill_id, cred)

    for name in bill_master_df["hostess_name"].unique():
        if name == "店":
            continue
        print("Processing bill for: ", name)
        df_hostess = filter_bill(name, bill_master_df)
        nw_rows = get_wage_comission(df_hostess)
        discount_rows = get_discounts(df_hostess)
        copied_template = copy_template(BILL_FILE, name, "Example_1")
        cpp_code = df_hostess["cp_code"].unique()[0]
        while True:
            try:
                add_new_rows_to_worksheet(
                    copied_template,
                    wage_comission_rows=nw_rows,
                    discount_rows=discount_rows,
                    cp_code=cpp_code,
                    month_str=month_string,
                )
                sleep(3)
                process_discounts(BILL_FILE, name, discount_rows, cpp_code)
                sleep(5)
                break
            except Exception as e:
                print(e)
                print("Error while processing bill for: ", name)
                sleep(waiting_time)
                total_errors = total_errors + 1
                waiting_time = waiting_time + 5
                if total_errors > 3:
                    print("Couldn't not process bill for: ", name)
                    break
                continue
    return
