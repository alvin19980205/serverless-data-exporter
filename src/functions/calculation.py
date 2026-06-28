import logging
import json
from functions.snowflake_query_helper_functions import create_snowpark_session

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_gsheet_usage(worksheets):
    """
    Calculates the used and available cells for a list of gspread worksheet objects.
    Google Sheets has a strict 10,000,000 cell limit per workbook.
    """
    cell_usage_by_tab = []
    
    try:
        logger.info(f"Calculating cell usage for {len(worksheets)} tabs.")
        for worksheet in worksheets:
            all_values = worksheet.get_all_values()
            
            # Get the total grid size for the current tab
            total_grid_rows = worksheet.row_count
            total_grid_cols = worksheet.col_count
            total_grid_cells = total_grid_rows * total_grid_cols
            
            # Efficiently count rows that have at least one piece of data
            non_empty_row_count = sum(1 for row in all_values if any(cell for cell in row))
            
            # Calculate used cells based on the specified logic
            cells_used_in_tab = non_empty_row_count * total_grid_cols
            cells_available_in_tab = total_grid_cells - cells_used_in_tab
            
            cell_usage_by_tab.append({
                "tab_name": worksheet.title,
                "cells_used": cells_used_in_tab,
                "cells_available_in_grid": cells_available_in_tab
            })
    except Exception as e:
        logger.error(f"ERROR: Could not calculate cell usage. Details: {e}")
        return [] 
    
    return cell_usage_by_tab

def get_export_data_size(export_query):
    """Calculates the exact cell footprint of the incoming Snowflake export data."""
    session = create_snowpark_session()
    try:
        # Executes the query and gets a DataFrame
        export_df = session.sql(export_query).to_pandas()
        num_rows, num_cols = export_df.shape
        
        # Calculate total cells, adding 1 to the row count for the header
        required_cells = (num_rows + 1) * num_cols
        
        return {
            "status": "success",
            "rows_found": num_rows,
            "columns_found": num_cols,
            "required_cells": required_cells
        }
    except Exception as e:
        logger.error(f"ERROR: Could not calculate export data size. Details: {e}")
        return {"required_cells": 0}

def validate_export_capacity(export_action, export_tab, gsheet_usage, export_size):
    """Validates if the planned ETL action will exceed Google's absolute workbook limits."""
    gsheet_cell_limit = 10_000_000
    total_gsheet_usage = sum(tab['cells_used'] for tab in gsheet_usage)
    projected_data_usage = 0

    if export_action == "append":
        projected_data_usage = total_gsheet_usage + export_size
    elif export_action == "replace":
        export_tab_usage = next((tab['cells_used'] for tab in gsheet_usage if tab['tab_name'] == export_tab), 0)
        projected_data_usage = (total_gsheet_usage - export_tab_usage) + export_size
    else:
        return {"error": f"Invalid export_action: '{export_action}'. Must be 'append' or 'replace'."}

    will_fit = projected_data_usage <= gsheet_cell_limit

    return {
        "status": "success",
        "will_fit": will_fit,
        "sheet_limit": gsheet_cell_limit,
        "current_total_used_cells": total_gsheet_usage,
        "required_cells_for_export": export_size,
        "projected_total_cells": projected_data_usage,
        "projected_available_cells": gsheet_cell_limit - projected_data_usage
    }

def _validate_export(sheet, export_action, tab_name, export_size):
    """Orchestrates the validation pipeline and raises exceptions if limits are breached."""
    logger.info("Validating export capacity against Google Workspace limits...")

    gsheet_worksheets = sheet.worksheets()
    cell_usage = get_gsheet_usage(gsheet_worksheets)

    validation_result = validate_export_capacity(export_action, tab_name, cell_usage, export_size)

    if "error" in validation_result:
        raise ValueError(validation_result["error"])

    if not validation_result.get("will_fit"):
        projected = validation_result.get('projected_total_cells', 'N/A')
        limit = validation_result.get('sheet_limit', 'N/A')
        raise ValueError(f"Export aborted: Projected cell count ({projected:,}) would exceed Google Sheet limit ({limit:,}).")

    logger.info("Validation successful. Export will safely fit inside the Google Sheet.")