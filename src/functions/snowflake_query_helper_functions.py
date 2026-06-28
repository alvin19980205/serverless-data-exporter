import tempfile
import os
import boto3
from typing import List, Optional
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSQLException

# Configuration loaded via environment variables to protect proprietary schemas
METADATA_DB = os.environ.get("METADATA_DB", "ETL_METADATA_DB")
METADATA_SCHEMA = os.environ.get("METADATA_SCHEMA", "PIPELINE_MANAGEMENT")

def create_snowpark_session() -> snowpark.Session:
    """Authenticates to Snowflake using Key-Pair auth securely stored in AWS Secrets Manager."""
    svc_name = os.environ.get("SNOWFLAKE_USER", "svc_export_user")
    secret_id = os.environ.get("SECRET_MANAGER_ID", f"sf/{svc_name}")
    
    client = boto3.client('secretsmanager')
    private_key = client.get_secret_value(SecretId=secret_id)['SecretString']
    
    # Auto-deleting temp file for secure key injection
    with tempfile.NamedTemporaryFile(delete=True) as temp_key_file:
        temp_key_file.write(private_key.encode())
        temp_key_file.flush() 

        session = (
        snowpark.Session.builder
            .configs({
                "account": os.environ.get("SNOWFLAKE_ACCOUNT", "mock_account.us-east-1"),
                "user": svc_name,
                "private_key_file": temp_key_file.name,
                "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "DEFAULT_WH")
            })
            .create()
        )
    return session

def snowpark_get_query_data(session: snowpark.Session, query: str) -> list[dict]:
    """Streams results row-by-row via local iterator to prevent Lambda OOM (Out Of Memory) errors."""
    if not query:
        raise ValueError("Cannot execute an empty query.")
    
    print(f"Executing query and streaming results: {query[:100]}...")
    
    data_frame = session.sql(query)
    all_results = []
    
    # Lazy evaluation avoids loading massive dataframes into RAM all at once
    for row in data_frame.to_local_iterator():
        all_results.append(row.as_dict())

    return all_results

def get_export_config(session: snowpark.Session, config_id: str) -> dict:
    """Fetches export configuration parameters dynamically."""
    table_name = f"{METADATA_DB}.{METADATA_SCHEMA}.GSHEET_EXPORT_CONFIG"
    columns_to_select = ['"SHARED_LINK"', '"EXPORT_QUERY"', '"TAB_NAME"', '"EXPORT_TYPE"', '"EXPORT_TABLE_NAME"']
    
    df = session.table(table_name).where(col('"CONFIG_ID"') == config_id).select(*columns_to_select)
    result = df.collect()
    
    if not result:
        raise ValueError(f"No configuration found for config_id: {config_id}")
    return result[0].as_dict()

def _sanitize_sql_for_subquery(sql: str) -> str:
    return (sql or "").strip().rstrip(";")

def get_query_size(session: snowpark.Session, query: str) -> int:
    """
    Probes the query schema to calculate exact destination cell footprints.
    Acts as an early-fail mechanism if permissions are missing.
    """
    if not query: return 0
    base_sql = _sanitize_sql_for_subquery(query)

    # Fast probe: compile the subquery to expose 42S02/permissions early without fetching data
    probe_sql = f"SELECT * FROM (\n{base_sql}\n) AS src LIMIT 1"
    try:
        df_probe = session.sql(probe_sql)
        num_cols = len(df_probe.schema.fields)
    except SnowparkSQLException as e:
        role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
        print({"error_on_probe": str(e), "role": role})
        raise

    # Count actual row volume
    count_sql = f"SELECT COUNT(*) AS ROW_COUNT FROM (\n{base_sql}\n) AS src"
    try:
        row = session.sql(count_sql).collect()[0]
        num_rows = int(row["ROW_COUNT"])
    except SnowparkSQLException as e:
        raise

    return (num_rows + 1) * num_cols

def get_config_list(session):
    """Orchestrator function: Fetches all IDs flagged for scheduled cron exports."""
    table_name = f"{METADATA_DB}.{METADATA_SCHEMA}.GSHEET_EXPORT_CONFIG"
    df = session.table(table_name).where(col('"SCHEDULED_EXPORT"') == True).select(col('"CONFIG_ID"'))
    return [r.as_dict().get('"CONFIG_ID"') or r[0] for r in df.collect()]

def log_gsheet_export(
    session, *, config_id: Optional[str], number_of_rows: Optional[int], 
    number_of_cols: Optional[int], execution_time_seconds: float, error_messages: Optional[str],
    export_type: Optional[str], user_name: Optional[str], export_name: Optional[str],
    export_query: Optional[str], sheet_url: Optional[str], sheet_name: Optional[str], tab_name: Optional[str]
):  
    """Pushes execution telemetry back to the Data Warehouse for pipeline monitoring."""
    if not config_id: return
    log_tbl = f"{METADATA_DB}.{METADATA_SCHEMA}.GSHEET_EXPORT_LOG"
    
    sql = f"""
        INSERT INTO {log_tbl} (
            CONFIG_ID, NUMBER_OF_ROWS, NUMBER_OF_COLS, EXECUTION_TIME_SECONDS, ERROR_MESSAGES,
            EXPORT_TYPE, USER_NAME, EXPORT_NAME, EXPORT_QUERY, SHEET_URL, SHEET_NAME, TAB_NAME
        ) VALUES (
            {_sql_literal(config_id)},
            {'NULL' if number_of_rows is None else int(number_of_rows)},
            {'NULL' if number_of_cols is None else int(number_of_cols)},
            {float(execution_time_seconds)},
            {_sql_literal(error_messages)}, {_sql_literal(export_type)},
            {_sql_literal(user_name)}, {_sql_literal(export_name)},
            {_sql_literal(export_query)}, {_sql_literal(sheet_url)},
            {_sql_literal(sheet_name)}, {_sql_literal(tab_name)}
        );
    """
    session.sql(sql).collect()

def _sql_literal(value: Optional[str]) -> str:
    if value is None: return "NULL"
    return "'" + str(value).replace("'", "''") + "'"

def get_user_name(session, config_id):
    try:
        table_name = f"{METADATA_DB}.{METADATA_SCHEMA}.GSHEET_EXPORT_CONFIG"
        result = session.sql(f"SELECT USER_NAME FROM {table_name} WHERE CONFIG_ID = '{config_id}'").collect()
        return str(result[0][0]) if result and result[0][0] else "N/A"
    except Exception:
        return "N/A"