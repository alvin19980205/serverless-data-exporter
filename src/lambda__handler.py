import json
import logging
import time
import os

from functions.google_export import google_export
from functions.helper_functions import _api_response, _normalize_event
from functions.snowflake_query_helper_functions import create_snowpark_session, get_config_list
from functions.schedule_export import schedule_export_for_config, delete_schedule_for_config
from functions.notification_helpers import send_slack_notification

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Main AWS Lambda entry point for Data Warehouse to Google Sheets exports."""
    try:
        data = _normalize_event(event or {})
        config_id = data.get("config_id")

        session = create_snowpark_session()
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "DEFAULT_WH")
        try:
            session.sql(f"USE WAREHOUSE {warehouse}").collect()
        except Exception:
            pass

        lambda_arn = getattr(context, "invoked_function_arn", None)

        # ROUTE 1: Manual Trigger or Setup via API
        if config_id:
            action = data.get("action")
            logger.info(f"Request for config_id={config_id}, action={action}")

            if action == "schedule_trigger":
                schedule_info = schedule_export_for_config(session, config_id, lambda_arn)
                return _api_response(200, {"message": "Schedule created.", "schedule": schedule_info})
            elif action == "delete_schedule":
                result = delete_schedule_for_config(session, config_id)
                return _api_response(200, result)
            else:
                result = google_export(config_id, action, session)
                return _api_response(200, result)
            
        # ROUTE 2: Automated CRON Execution (EventBridge)
        logger.info("No config_id provided. Processing all scheduled exports.")
        try:
            config_ids = get_config_list(session)
            if not config_ids:
                return _api_response(200, {"message": "No scheduled exports."})
            
            results = []
            for cid in config_ids:
                start_time = time.time()
                try:
                    result = google_export(cid, "trigger_export", session)
                    results.append({"config_id": cid, "status": "success"})
                    send_slack_notification(session, is_success=True, config=result.get("config"), rows=result.get("rows", 0), elapsed=time.time() - start_time)
                except Exception as e:
                    logger.error(f"Export FAILED for config_id={cid}: {e}")
                    results.append({"config_id": cid, "status": "failed", "error": str(e)})
                    send_slack_notification(session, is_success=False, config={"CONFIG_ID": cid}, error_msg=e)
                    
            return _api_response(200, {"message": "Scheduled exports processed.", "results": results})
        finally:
            session.close()

    except Exception as e:
        logger.exception("An unexpected error occurred.")
        return _api_response(500, {"error": "Internal Server Error", "details": str(e)})