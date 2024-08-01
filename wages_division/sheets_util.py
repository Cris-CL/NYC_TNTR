def clear_formatting(FILE, sheet_name):
    wsht = FILE.worksheet(sheet_name)
    sheetId = int(wsht._properties["sheetId"])
    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheetId
                    },
                    "cell": {
                        "userEnteredFormat": {}
                    },
                    "fields": "userEnteredFormat"
                }
            }
        ]
    }
    try:
        FILE.batch_update(body)
    except:
        print(f"Error in clearing format on {sheet_name}")
