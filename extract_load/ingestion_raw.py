import snowflake.connector
import os
from pathlib import Path
from dotenv import load_dotenv

from logger import setup_logger
from extract_load.raw_table_schemas import RAW_TABLE_SCHEMAS

logger = setup_logger("logs/pipeline.log")
load_dotenv("config.env")

def load_tables():
    """
    Main function to load all tables to Snowflake from RAW_DATA_DIR
    """
    cursor_raw = _init_snowflake_conn(
        schema="raw"
    )
    for file in Path(os.getenv("RAW_DATA_DIR")).iterdir():
        if file.is_file():
            _upload_csv_to_raw(cursor=cursor_raw, file_path=file)

def _init_snowflake_conn(
        schema: str = None) -> snowflake.connector.cursor.SnowflakeCursor:
    """
    Initialised connection to snowflake with optional schema specification
    Returns cursor for downstream SQL executions
    """
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SF_ACCOUNT"),
            user=os.getenv("SF_USERNAME"),
            password=os.getenv("SF_PASSWORD"),
            warehouse=os.getenv("SF_WAREHOUSE"),
            role=os.getenv("SF_ROLE"),
            database=os.getenv("SF_DATABASE"),
            schema=schema
        )

        cursor = conn.cursor()
        if not cursor:
            logger.error("Cursor object is None")
            return
        # Test
        user, role = cursor.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE()").fetchone()
        logger.info(
            "Initiated Snowflake Connection. "
            "CURRENT_USER: {}, CURRENT_ROLE: {}".format(
                user, role
        ))
        return cursor
    
    except Exception as e:
        logger.error(f"Error enountered in _init_snowflake_conn: {e}")

def _upload_csv_to_raw(
        cursor: snowflake.connector.cursor.SnowflakeCursor,
        file_path: Path
) -> None:
    """
    Uploads a local file to raw schema on snowflake
    """
    # Define variables
    table_name = file_path.stem
    stage_name = os.getenv("SF_RAW_STAGE_NAME")
    ddl = RAW_TABLE_SCHEMAS.get(file_path.name, None)
    abs_file_path = os.getcwd() / file_path

    logger.info(f"Stage and copy file: {file_path} as table: {table_name}")

    # 1. Create Table if not exists
    logger.info(f"[1/3] Ensuring table {table_name} exists...")
    if not ddl:
        logger.error("ddl statement not found for key ", file_path.name)
        return
    ddl_result = cursor.execute(ddl).fetchone()
    logger.debug(f"      Status: {ddl_result[0]}")

    if "already exists, statement succeeded." in ddl_result[0]:
        logger.info("Early return since table exists")
        return

    # 2. Stage the file into Snowflake's internal stage
    logger.info(f"[2/3] Staging file {abs_file_path}")
    put_result = cursor.execute(f"""
        PUT file:///{abs_file_path} @{stage_name}
        AUTO_COMPRESS=TRUE OVERWRITE=TRUE
    """).fetchall()
    logger.debug(f"      Status: {put_result[0][6]}")

    # 3. COPY INTO the target table by bulk loading
    logger.info(f"[3/3] Loading into OLIST_DB.RAW.{table_name}")
    copy_result = cursor.execute(f"""
        COPY INTO OLIST_DB.RAW.{table_name}
        FROM @{stage_name}/{file_path.name}.gz
        FILE_FORMAT = (
            TYPE                         = 'CSV'
            SKIP_HEADER                  = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF                      = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL          = TRUE,
            ERROR_ON_COLUMN_COUNT_MISMATCH  = FALSE
        )
        ON_ERROR = 'CONTINUE'   -- log errors but don't fail, good for raw layer
        PURGE = TRUE            -- clean up stage after load
    """).fetchall()

    # Verify indexes here https://docs.snowflake.com/en/user-guide/tutorials/data-load-internal-tutorial#create-stage-objects 
    for row in copy_result:
        logger.debug(f"  File              : {row[0]}")
        logger.debug(f"  Status            : {row[1]}")
        logger.debug(f"  Rows parsed       : {row[2]}")
        logger.debug(f"  Rows loaded       : {row[3]}")
        logger.debug(f"  Error limit       : {row[4]}")
        logger.debug(f"  Errors seen       : {row[5]}")
