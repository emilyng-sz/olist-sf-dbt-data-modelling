from extract_load.ingestion_raw import load_tables

load_tables(
    full_reload=True,
    reload_schema=True,
    check_exists=False
)
