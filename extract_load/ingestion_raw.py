import snowflake.connector
import os
import csv
from pathlib import Path
from dotenv import load_dotenv

from extract_load.logger import setup_logger
from extract_load.raw_table_schemas import RAW_TABLE_SCHEMAS

logger = setup_logger("logs/pipeline.log")
load_dotenv("config.env")

def load_tables(
        tables_to_load: list[str] = [],
        full_reload: bool = True,
        check_exists: bool = True,
        reload_schema: bool = True
):
    """
    Main function to load all tables to Snowflake from RAW_DATA_DIR
    full_reload: True if overwrite, False if append
    check_exists: SQL PUT and COPY INTO will not run if table already exists
    """
    logger.info("Loading tables with full_reload {} and check_exists {}".format(
        full_reload, check_exists
    ))
    cursor_raw = _init_snowflake_conn(
        schema=os.getenv("SF_RAW_SCHEMA_NAME")
    )
    # Check if cursor is valid
    if not cursor_raw:
        logger.error(f"Function load_tables() exited due to invalid snowflake cursor")
        return

    # Define variables
    database_name = os.getenv("SF_DATABASE")
    schema_name = os.getenv("SF_RAW_SCHEMA_NAME")
    raw_stage_name = os.getenv("SF_RAW_STAGE_NAME")
    stage_name = f"{database_name}.{schema_name}.{raw_stage_name}"

    # 1. Create Schema if not exists
    logger.info(f"[1/2] Ensuring schema {schema_name} exists...")
    schema_result = cursor_raw.execute(
        f"CREATE SCHEMA IF NOT EXISTS {schema_name}").fetchone()
    logger.debug(f"      Status: {schema_result[0]}")

    # 2. Create Stage if not exists
    logger.info(f"[2/2] Ensuring stage {stage_name} exists...")
    stage_result = cursor_raw.execute(
        f"CREATE STAGE IF NOT EXISTS {stage_name}").fetchone()
    logger.debug(f"      Status: {stage_result[0]}")
    
    for file in Path(os.getenv("RAW_DATA_DIR")).iterdir():
        if file.is_file() and \
            (file.stem in tables_to_load if tables_to_load else True):
            _upload_csv_to_raw(
                cursor=cursor_raw,
                database_name=database_name,
                schema_name=schema_name,
                stage_name=raw_stage_name,
                file_path=file,
                full_reload=full_reload,
                check_exists=check_exists,
                reload_schema=reload_schema
            )

def _environ_variables_complete() -> bool:
    required_vars = ["SF_ACCOUNT", "SF_USERNAME", "SF_PASSWORD", "SF_WAREHOUSE", "SF_ROLE", "SF_DATABASE"]
    missing = [v for v in required_vars if os.getenv(v) is None]
    if missing:
        logger.error(f"Missing environment variables: {missing}")
        return False
    return True
    
def _init_snowflake_conn(
        schema: str) -> snowflake.connector.cursor.SnowflakeCursor:
    """
    Initialised connection to snowflake with optional schema specification
    Returns cursor for downstream SQL executions
    """
    try:
        if not _environ_variables_complete():
            raise Exception("Missing environ variables")

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
        if "Multi-factor authentication is required" in str(e):
            logger.warning("May have to run bypass MFA for database. "
                           "Consider: `ALTER USER olist_svc SET MINS_TO_BYPASS_MFA = 0`")
        return

def _upload_csv_to_raw(
        cursor: snowflake.connector.cursor.SnowflakeCursor,
        database_name: str,
        schema_name: str,
        stage_name: str,
        file_path: Path,
        full_reload: bool = True,
        reload_schema: bool = True,
        check_exists: bool = True,
) -> None:
    """
    Uploads a local file to raw schema on snowflake
    if reload_schema is True, will first drop table before running ddl statement
    """
    # Check if cursor is valid
    if not cursor:
        return ConnectionError("Snowflake connector failed to initialise")
    
    # Define variables
    table_name = file_path.stem
    ddl = RAW_TABLE_SCHEMAS.get(file_path.name, None)
    abs_file_path = os.getcwd() / file_path

    # 0. Optionally drop table before creation if reload_schema is True
    if reload_schema:
        try: 
            logger.info(f"[0/4] DROP table {table_name} if exists before creation (reload_schema=True.)")
            drop_result = cursor.execute(
                f"DROP TABLE IF EXISTS {database_name}.{schema_name}.{table_name}"
            ).fetchone()
            logger.debug(f"      Status: {drop_result[0]}")
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {e}\n"
                         "Continuing with load attempt.")

    # 1. Create Table if not exists
    logger.info(f"[1/4] Ensuring table {table_name} exists...")
    if not ddl:
        logger.error("ddl statement not found for key ", file_path.name)
        return
    ddl_result = cursor.execute(
        ddl.replace("{database_name}", database_name)\
           .replace("{schema_name}", schema_name)).fetchone()
    logger.debug(f"      Status: {ddl_result[0]}")

    if check_exists and "already exists, statement succeeded." in ddl_result[0]:
        logger.info("Early return since table exists")
        return

    # 2. Stage the file into Snowflake's internal stage
    logger.info(f"[2/4] Staging file {abs_file_path}")
    put_result = cursor.execute(f"""
        PUT file:///{abs_file_path} @{database_name}.{schema_name}.{stage_name}
        AUTO_COMPRESS=TRUE OVERWRITE=TRUE
    """).fetchall()
    logger.debug(f"      Status: {put_result[0][6]}")

    # 3. Validate the staged file against the target table schema before loading
    logger.info(f"[3/4] Validating staged file against target table schema")
    validated = _validate_stage_file(
        cursor,
        database_name=database_name,
        schema_name=schema_name,
        stage_name=stage_name,
        table_name=table_name,
        file_name=file_path.name
    )
    if not validated:
        logger.error(
            f"Pre-check validation failed for {file_path.name}. "
            f"Please resolve error and enable reload_schema=True and check_exists=False before rerunning"
        )
        return

    # 4. COPY INTO the target table by bulk loading
    logger.info(f"[4/4] Loading into {database_name}.{schema_name}.{table_name}")
    if full_reload:
        logger.info(f"First truncating table: {table_name}")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
    copy_result = cursor.execute(f"""
        COPY INTO {database_name}.{schema_name}.{table_name}
        FROM @{database_name}.{schema_name}.{stage_name}/{file_path.name}.gz
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
    total_errors = 0
    for row in copy_result:
        logger.debug(f"  File              : {row[0]}")
        logger.debug(f"  Status            : {row[1]}")
        logger.debug(f"  Rows parsed       : {row[2]}")
        logger.debug(f"  Rows loaded       : {row[3]}")
        logger.debug(f"  Errors seen       : {row[5]}")

        rows_dropped = row[2] - row[3]
        total_errors += rows_dropped

    # This should not occur since file is validate in step 3
    if total_errors > 0:
        logger.warning(
            f"{total_errors} row(s) rejected in loading of "
            f"Table : {database_name}.{schema_name}.{table_name}\n"
        )

def _validate_stage_file(
        cursor,
        database_name: str,
        schema_name: str,
        stage_name: str,
        table_name: str,
        file_name: str,
    ) -> bool:
    """
    Validates a staged file against the target table schema
    without loading any data.

    Returns True if file is clean, False if errors were found.
    The caller decides whether to proceed with loading or abort.
    """
    # Preview first 10 rows to ensure file can be loaded
    try:
        preview = cursor.execute(f"""
            COPY INTO {database_name}.{schema_name}.{table_name}
            FROM @{database_name}.{schema_name}.{stage_name}/{file_name}
            FILE_FORMAT = (
                TYPE                            = 'CSV'
                SKIP_HEADER                     = 1
                FIELD_OPTIONALLY_ENCLOSED_BY    = '"'
                NULL_IF                         = ('', 'NULL', 'null')
                EMPTY_FIELD_AS_NULL             = TRUE
                ERROR_ON_COLUMN_COUNT_MISMATCH  = FALSE
            )
            VALIDATION_MODE = RETURN_10_ROWS   -- parse first 10 rows, write nothing
        """).fetchall()

        logger.debug(f"  Check 1 PASS. {len(preview)} sample rows parsed successfully")

    except Exception as e:
        # If preview fails, abort
        logger.error(
            f"  Check 1 FAILED: File cannot be parsed at all.\n"
            f"  Error: {e}\n"
            f"  Aborting load for {table_name}."
        )
        return False

    # Scan entire file for errors without loading
    error_results = cursor.execute(f"""
        COPY INTO {database_name}.{schema_name}.{table_name}
        FROM @{database_name}.{schema_name}.{stage_name}/{file_name}
        FILE_FORMAT = (
            TYPE                            = 'CSV'
            SKIP_HEADER                     = 1
            FIELD_OPTIONALLY_ENCLOSED_BY    = '"'
            NULL_IF                         = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL             = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH  = FALSE
        )
        VALIDATION_MODE = RETURN_ERRORS    -- scan all rows, return failures only
    """).fetchall()

    if not error_results:
        logger.debug(
            f"  Check 2 PASS. Rows parsed successfully. Safe to load."
        )
        return True

    # If there are errors, log every error with full detail before deciding
    logger.warning(
        f"  Check 2 FAILED: {len(error_results)} row(s) would fail on load to"
        f" {database_name}.{schema_name}.{table_name}"
    )

    rejected_path = Path(f"logs/pre_check_rejected_{table_name}.csv")

    with open(rejected_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'line_number', 'column_name',
            'error_message', 'rejected_record'
        ])

        for err in error_results:
            error_msg       = err[0]
            line_number     = err[2]
            column_name     = err[8] if err[8] else 'unknown'
            rejected_record = err[11]

            writer.writerow([
                line_number, column_name,
                error_msg, rejected_record
            ])

    logger.info(
        f"Pre-check rejected rows written to: {rejected_path.resolve()}"
    )

    return False
