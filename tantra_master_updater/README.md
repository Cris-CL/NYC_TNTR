# tantra_master_updater

A Cloud Function that refreshes **master/reference tables in BigQuery from Google Sheets**. Unlike the Drive→bucket→BigQuery flow (which handles uploaded `.xlsx` data files), this service reads curated Google Sheets — wages, prices, extra charges, online orders — transforms them, and writes a fresh version to BigQuery. It is invoked by a `POST` whose JSON body names which dataset to update.

This is a **sibling pipeline** to the `.xlsx` ingestion path, not part of its trigger chain; it keeps the supporting/master data in BigQuery up to date.

## Request contract

`POST /update_master` with a JSON body:
```json
{ "type": "wages|prices|extra|online", "sheetName": "<worksheet name>" }
```
The `type` selects which updater runs; `sheetName` (used by `wages`/`prices`) names the worksheet to read. A `GET` returns a small HTML health message.

## Files

### `main.py` — Router (Cloud Function entry point)
Flask handler on `/update_master`. On `POST` it reads `type` from the JSON body and dispatches to the matching updater (importing it lazily):
- `extra`  → `extra.update_extra("Extra")` then `sleep(100)`
- `wages`  → `wages.update_wages(sheetName)`
- `prices` → `prices.update_prices(sheetName)`
- `online` → `online_orders.update_online_sales("Transfer")`

Each branch is wrapped so an error is printed rather than crashing the whole handler; it returns `{"status": "success"}` / 200.

### `wages.py` — Hostess wages → BigQuery
- `get_wages(sheet_name)` — opens the wages sheet (`WAGES_SHEET_ID`), reads the worksheet into a DataFrame, renames to a fixed 7-column schema (`hostess_name`, `real_name`, `wage_class`, `cp_code`, `hour_a`, `hour_b`, `YEAR_MONTH`), cleans integer columns (strips commas/spaces, parentheses → negative, `.00` removal), casts types, blanks → `None`, and stamps an `UPLOADED` timestamp.
- `update_wages(sheet_name)` — skips when `sheet_name == "MASTER"`; otherwise appends the DataFrame to `{DATASET}.{HOSTESS_WAGE}` (`if_exists="append"`), then calls `replace_wages`.
- `replace_wages(year_month)` — deletes older rows for that `YEAR_MONTH` keeping only the latest `UPLOADED` batch (append-then-prune = atomic-ish "replace this month").

Env: `WAGES_SHEET_ID`, `HOSTESS_WAGE`, `DATASET`, `PROJECT_ID`.

### `prices.py` — Product price master → BigQuery
- `update_prices(sheet_name)` — skips `MASTER`; opens `PRICES_SHEET_ID`, maps the Japanese column headers to English names via `products_dict`, drops rows with empty `product_code`, cleans/casts integer columns, stamps `UPLOADED`, and appends to `{DATASET}.{PRODUCT_M}`. Then calls `replace_prices` with the sheet's `year_month`.
- `replace_prices(year_month)` — deletes older rows for that `year_month` keeping only the latest `UPLOADED` batch.

Env: `PRICES_SHEET_ID`, `PRODUCT_M`, `DATASET`, `PROJECT_ID`.

### `extra.py` — Extra charges → BigQuery
- `update_extra(sheet_name)` — skips `MASTER`; calls `replace_extra()` first, reads the `EXTRA_SHEET_ID` worksheet, fixes a 10-column schema (`DAY`, `NAME`, `CODE`, `tip_ret`, `x_20`, `x_395`, `okuri`, `disc`, `adv`, `notes`), cleans integer columns, drops rows with no `DAY`, stamps `UPLOADED`, replaces `"Loading..."` codes using the hostess-code map, then writes to `{DATASET}.{EXTRA_H}` with `if_exists="replace"`, and calls `replace_extra()` again.
- `replace_extra()` — deletes rows older than the latest `UPLOADED` in the extra table.
- `get_hostess_code(master_id)` — opens the wages master sheet's `MASTER` worksheet and builds a `{name: code}` dict (columns A→D).
- `replace_loading_codes(df, code_dict)` — fills in any `CODE` value containing "oading" (i.e. "Loading...") with the correct code from the dict.

Env: `EXTRA_SHEET_ID`, `EXTRA_H`, `DATASET`, `PROJECT_ID`, `WAGES_SHEET_ID`.

### `online_orders.py` — Online sales → BigQuery
- `update_online_sales(sheet_name)` — opens `OSALES_SHEET_ID`, reads the worksheet, uses the first row as headers, normalizes column names (spaces/parens/slashes → underscores, lowercase), keeps the first 15 columns, casts `created`/`amount`/`fees`/`net`, drops rows with no `transfer`, stamps `UPLOADED`, and writes to `{DATASET}.{ONLINE_TABLE}` with `if_exists="replace"`. Reads its env vars inside the function.

Env: `OSALES_SHEET_ID`, `ONLINE_TABLE`, `DATASET`, `PROJECT_ID`.

### `requirements.txt`
Pinned dependencies for the deployed function (functions-framework, flask, gspread, pandas, pandas-gbq, google-cloud-bigquery, openpyxl, etc.).

## How the pieces interact
- `main.py` is the only entry point; each `*.py` module is an independent updater it dispatches to by `type`.
- The three "transactional" tables (`wages`, `prices`) follow an **append-then-prune** pattern: append the new batch with an `UPLOADED` timestamp, then `replace_*` deletes the older versions for that key (`YEAR_MONTH` / `year_month`), so BigQuery ends up holding only the latest version per period.
- `extra` and `online` instead use `if_exists="replace"` to overwrite the whole table (extra additionally prunes by `UPLOADED`).
- `extra.py` depends on the wages master sheet (via `get_hostess_code`) to resolve hostess codes.

## Authentication
All updaters use Application Default Credentials (`google.auth.default()`), authorized for both `gspread` (Sheets) and BigQuery; the runtime service account needs access to the source spreadsheets and the target BigQuery dataset.
