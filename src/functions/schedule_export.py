import json
import logging
import re
import hashlib
import os
import boto3
import botocore.exceptions

logger = logging.getLogger(__name__)

# Leverage Boto3 EventBridge Scheduler
scheduler_client = boto3.client("scheduler")

# Parameterize configurations
CONFIG_TABLE_FQN = f"{os.environ.get('METADATA_DB', 'DB')}.{os.environ.get('METADATA_SCHEMA', 'SCHEMA')}.GSHEET_EXPORT_CONFIG"
LAMBDA_ROLE_ARN = os.environ.get("LAMBDA_EXECUTION_ROLE_ARN", "arn:aws:iam::123456789012:role/snowflake-gsheet-exporter")

def _build_schedule_name(user_name: str, export_table_name: str, config_id: str, max_length: int = 64) -> str:
    """Builds a deterministic, AWS-compliant resource name for the EventBridge schedule."""
    base = f"{export_table_name}_{config_id}"
    cleaned = re.sub(r"[^A-Za-z0-9_\-\.@]", "-", base)

    if len(cleaned) <= max_length:
        return cleaned

    suffix = hashlib.md5(base.encode("utf-8")).hexdigest()[:8]
    return cleaned[: max_length - 1 - len(suffix)] + "-" + suffix

def _get_config_metadata(session, config_id: str):
    """Fetches scheduling metadata directly from the source of truth database."""
    query = f"SELECT CONFIG_ID, USER_NAME, EXPORT_TABLE_NAME, EXPORT_FREQUENCY FROM {CONFIG_TABLE_FQN} WHERE CONFIG_ID = '{config_id}'"
    rows = session.sql(query).collect()
    
    if not rows:
        raise ValueError(f"No configuration found for CONFIG_ID={config_id}")

    row = rows[0]
    if not row["USER_NAME"] or not row["EXPORT_TABLE_NAME"] or not row["EXPORT_FREQUENCY"]:
        raise ValueError("Missing critical metadata for scheduling.")

    return {
        "config_id": config_id,
        "user_name": str(row["USER_NAME"]),
        "export_table_name": str(row["EXPORT_TABLE_NAME"]),
        "cron_id": str(row["EXPORT_FREQUENCY"]),
    }

def _ensure_cron_expression(cron_id: str) -> str:
    """Standardizes UI inputs to AWS EventBridge cron formats."""
    cron_stripped = cron_id.strip()
    if cron_stripped.lower().startswith("cron("):
        return cron_stripped
    return f"cron({cron_stripped})"

def schedule_export_for_config(session, config_id: str, lambda_arn: str, schedule_group_name: str = "data-export-pipelines"):
    """
    Creates or updates an AWS EventBridge Schedule that will invoke this very Lambda 
    function on a recurring basis, passing the specific config_id as the payload.
    """
    meta = _get_config_metadata(session, config_id)
    schedule_expression = _ensure_cron_expression(meta["cron_id"])
    schedule_name = _build_schedule_name(meta["user_name"], meta["export_table_name"], config_id)

    target_input = json.dumps({
        "config_id": config_id,
        "action": "trigger_export",
    })

    MAX_JITTER_MINUTES = 15

    try:
        scheduler_client.create_schedule(
            Name=schedule_name,
            GroupName=schedule_group_name,
            ScheduleExpression=schedule_expression,
            FlexibleTimeWindow={"Mode": "FLEXIBLE", "MaximumWindowInMinutes": MAX_JITTER_MINUTES},
            State="ENABLED",
            Target={"Arn": lambda_arn, "RoleArn": LAMBDA_ROLE_ARN, "Input": target_input},
        )
        action_taken = "created"
        
    except botocore.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ConflictException":
            # Schedule already exists -> update instead
            scheduler_client.update_schedule(
                Name=schedule_name,
                GroupName=schedule_group_name,
                ScheduleExpression=schedule_expression,
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Target={"Arn": lambda_arn, "RoleArn": LAMBDA_ROLE_ARN, "Input": target_input},
            )
            action_taken = "updated"
        else:
            raise

    logger.info(f"EventBridge Schedule '{schedule_name}' {action_taken} for config_id={config_id}.")
    return {"schedule_name": schedule_name, "status": action_taken}

def delete_schedule_for_config(session, config_id: str, schedule_group_name: str = "data-export-pipelines"):
    """Tears down the AWS EventBridge infrastructure for deleted pipelines."""
    meta = _get_config_metadata(session, config_id)
    schedule_name = _build_schedule_name(meta["user_name"], meta["export_table_name"], config_id)

    try:
        scheduler_client.delete_schedule(Name=schedule_name, GroupName=schedule_group_name)
        return {"status": "success", "message": f"Schedule '{schedule_name}' removed."}
    except scheduler_client.exceptions.ResourceNotFoundException:
        return {"status": "not_found", "message": "Schedule did not exist."}