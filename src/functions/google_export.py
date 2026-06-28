import logging
import gspread
import time
from functions.snowflake_query_helper_functions import get_export_config, snowpark_get_query_data, get_query_size, log_gsheet_export
from functions.google_export_helper_functions import replace_data_in_sheet, append_data_to_sheet, get_google_secret, _validate_export
from functions.helper_functions import _retry_gspread

logger = logging.getLogger()

def google_export(config_id, action, session):
    """High-level controller for Google Sheets actions."""
    if not config_id: raise ValueError("config_id is required.")
    
    t0 = time.time()
    _rows = _cols = 0
    _etype = _ename = _eq = _surl = _sname = _tab = _err = None

    try:
        gc = gspread.service_account_from_dict(get_google_secret())
        config = get_export_config(session, config_id)
        
        _surl = config.get("SHARED_LINK")
        _eq = config.get("EXPORT_QUERY")
        _tab = config.get("TAB_NAME")
        _etype = config.get("EXPORT_TYPE")
        _ename = config.get("EXPORT_TABLE_NAME")

        sheet = _retry_gspread(lambda: gc.open_by_url(_surl))
        _sname = sheet.title
        
        if action == "verify_access":
            tabs = [ws.title for ws in _retry_gspread(lambda: sheet.worksheets())]
            return {"status": "success", "fileName": sheet.title, "sheetTabs": tabs}

        elif action == "trigger_export":
            # 1. Pre-validate limits
            export_size = get_query_size(session, _eq)
            _validate_export(sheet, _etype, _tab, export_size)

            # 2. Fetch data (lazy loading)
            data = snowpark_get_query_data(session, _eq)
            _rows, _cols = len(data), len(data[0]) if data else 0
            
            # 3. Write to Sheets
            if _etype == "replace":
                message = replace_data_in_sheet(sheet, _tab, data)
            elif _etype == "append":
                message = append_data_to_sheet(sheet, _tab, data)
                
            return {"status": "success", "message": message, "rows": _rows}
            
    except Exception as e:
        _err = str(e)
        logger.error(f"Error google_export: {_err}")
        raise
    finally:
        try:
            log_gsheet_export(session, config_id=config_id, number_of_rows=_rows, execution_time_seconds=(time.time() - t0))
        except Exception:
            pass