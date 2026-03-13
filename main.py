from pathlib import Path
from dotenv import load_dotenv

from extract_load.logger import setup_logger
from extract_load.raw_table_schemas import RAW_TABLE_SCHEMAS
from extract_load.ingestion_raw import load_tables

#logger = setup_logger("logs/pipeline.log")
#load_dotenv("config.env")

load_tables(
    full_reload=True,
    check_exists=True
)