import logging
import os

logger = logging.getLogger()

# Pull database routing from Environment Variables
NOTIFY_DB = os.environ.get("NOTIFICATION_DB", "CORE_INFRASTRUCTURE_DB")
NOTIFY_SCHEMA = os.environ.get("NOTIFICATION_SCHEMA", "PUBLIC")

def format_slack_username(raw_name):
    """Normalizes 'First Last' into enterprise Slack handle formats 'first.last'."""
    if not raw_name or raw_name == "N/A":
        return None
    parts = str(raw_name).strip().split()
    formatted = ".".join(parts).lower()
    return f"{formatted}" if formatted else None

def get_recipients_list(config):
    """Dynamically parses and constructs the notification routing list."""
    users = set()
    owner = format_slack_username(config.get("USER_NAME"))
    if owner: users.add(owner)
    
    shared = config.get("SHARED_USER")
    if shared and shared != "N/A":
        for name in str(shared).split(','):
            formatted = format_slack_username(name)
            if formatted: users.add(formatted)
            
    return list(users) if users else ["data_admin"]

def send_slack_notification(session, is_success, config, error_msg=None, rows=0, elapsed=0):
    """Calls a Snowflake external function/procedure to route webhooks to Slack."""
    try:
        # Defensive programming: Ensure procedure exists before calling
        check_proc = session.sql(f"""
            SELECT COUNT(*) FROM {NOTIFY_DB}.INFORMATION_SCHEMA.PROCEDURES 
            WHERE PROCEDURE_NAME = 'SEND_SLACK_NOTIFICATION'
        """).collect()
        
        if not check_proc or check_proc[0][0] == 0:
            logger.warning("Slack routing procedure not found. Bypassing notification.")
            return

        recipients = get_recipients_list(config)
        recipients_str = ', '.join([f"'{r}'" for r in recipients])
        client_name = config.get("USER_NAME", "Unknown")
        table_name = config.get("EXPORT_TABLE_NAME", "Unknown")
        
        if is_success:
            msg = (f"✅ *Export Success*: {table_name}\n"
                   f"• *User*: {client_name}\n"
                   f"• *Rows Extracted*: {rows:,}\n"
                   f"• *Compute Time*: {elapsed:.1f}s")
            icon = ":white_check_mark:"
            title = "Pipeline Success"
        else:
            msg = (f"❌ *Export Failed*: {table_name}\n"
                   f"• *User*: {client_name}\n"
                   f"• *System Error*: `{str(error_msg)[:200]}`")
            icon = ":warning:"
            title = "Pipeline Error"

        # Execute Snowflake procedure to trigger API Gateway webhook
        session.sql(f"""
            CALL {NOTIFY_DB}.{NOTIFY_SCHEMA}.SEND_SLACK_NOTIFICATION(
                to_variant(array_construct({recipients_str})),
                '{msg.replace("'", "''")}',
                '{title}', '{icon}'
            )
        """).collect()
        
    except Exception as e:
        logger.error(f"Failed to route Slack notification: {e}")