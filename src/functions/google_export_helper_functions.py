import json
import boto3
import os
from gspread.exceptions import WorksheetNotFound

def get_google_secret() -> dict:
    """Retrieves Google Workspace Service Account JSON from AWS Secrets Manager."""
    secret_id = os.environ.get("GOOGLE_SECRET_ID", "google/sheets_service_account")
    client = boto3.client('secretsmanager')
    secret_string = client.get_secret_value(SecretId=secret_id)['SecretString']
    return json.loads(secret_string)

def _validate_export(sheet, export_type, tab_name, required_cells):
    """Ensures export won't exceed Google Sheet's 10 million cell limit."""
    CELL_LIMIT = 10_000_000
    if required_cells > CELL_LIMIT:
        raise ValueError(f"Export exceeds Google Sheets limit ({required_cells} > 10M cells).")

def replace_data_in_sheet(sheet, tab_name, data):
    """Replaces all data in the target worksheet."""
    try:
        worksheet = sheet.worksheet(tab_name)
    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=tab_name, rows=100, cols=20)
    
    worksheet.clear()
    if data:
        headers = list(data[0].keys())
        values = [list(row.values()) for row in data]
        worksheet.update([headers] + values)
    return "Data replaced successfully."

def append_data_to_sheet(sheet, tab_name, data):
    """Appends data to the target worksheet."""
    worksheet = sheet.worksheet(tab_name)
    if data:
        values = [list(row.values()) for row in data]
        worksheet.append_rows(values)
    return "Data appended successfully."