"""
table_schemas.py
----------------
Central registry of all RAW schema table definitions for the Olist dataset.

Each entry maps a table name to its Snowflake CREATE TABLE DDL.
All source columns are stored as VARCHAR in the raw layer regardless of their
original type — casting and type enforcement is deferred to the staging layer.

Audit columns added to every table:
  - _ingested_at : DB-enforced timestamp of when the row was loaded
  - _source_file : Snowflake stage filename for row-level lineage
"""

RAW_TABLE_SCHEMAS: dict[str, str] = {
    "olist_customers_dataset.csv": """
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.olist_customers_dataset (
            CUSTOMER_ID                 VARCHAR     NOT NULL,   -- Primary Key
            CUSTOMER_UNIQUE_ID          VARCHAR     NOT NULL,
            CUSTOMER_ZIP_CODE_PREFIX    VARCHAR     NOT NULL,   -- FK → DIM_ZIP_CODE
            CUSTOMER_CITY               VARCHAR     NOT NULL,
            CUSTOMER_STATE              VARCHAR     NOT NULL
        )
    """,
    "olist_orders_dataset.csv": """
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.olist_orders_dataset (
            ORDER_ID                        VARCHAR     NOT NULL,   -- Primary Key
            CUSTOMER_ID                     VARCHAR     NOT NULL,   -- FK → DIM_CUSTOMERS
            ORDER_STATUS                    VARCHAR     NOT NULL,
            ORDER_PURCHASE_TIMESTAMP        VARCHAR     NOT NULL,
            ORDER_APPROVED_AT               VARCHAR,               -- 160 nulls expected
            ORDER_DELIVERED_CARRIER_DATE    VARCHAR,               -- 1783 nulls expected
            ORDER_DELIVERED_CUSTOMER_DATE   VARCHAR,               -- 2965 nulls expected
            ORDER_ESTIMATED_DELIVERY_DATE   VARCHAR     NOT NULL
        )
    """,
    "olist_order_items_dataset.csv": """
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.olist_order_items_dataset (
            ORDER_ID                VARCHAR     NOT NULL,   -- Composite PK (with ORDER_ITEM_ID)
            ORDER_ITEM_ID           VARCHAR     NOT NULL,   -- Composite PK (with ORDER_ID)
            PRODUCT_ID              VARCHAR     NOT NULL,
            SELLER_ID               VARCHAR     NOT NULL,
            SHIPPING_LIMIT_DATE     VARCHAR     NOT NULL,
            PRICE                   VARCHAR     NOT NULL,
            FREIGHT_VALUE           VARCHAR     NOT NULL
        )
    """,
    "olist_order_payments_dataset.csv": """
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.olist_order_payments_dataset (
            ORDER_ID                VARCHAR     NOT NULL,   -- Composite PK (with PAYMENT_SEQUENTIAL)
            PAYMENT_SEQUENTIAL      VARCHAR     NOT NULL,   -- Composite PK (with ORDER_ID)
            PAYMENT_TYPE            VARCHAR     NOT NULL,
            PAYMENT_INSTALLMENTS    VARCHAR     NOT NULL,
            PAYMENT_VALUE           VARCHAR     NOT NULL
        )
    """,
    "olist_order_reviews_dataset.csv": """
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.olist_order_reviews_dataset (
            REVIEW_ID                   VARCHAR     NOT NULL,
            ORDER_ID                    VARCHAR     NOT NULL,
            REVIEW_SCORE                VARCHAR     NOT NULL,
            REVIEW_COMMENT_TITLE        VARCHAR,               -- 87656 nulls expected
            REVIEW_COMMENT_MESSAGE      VARCHAR,               -- 58247 nulls expected
            REVIEW_CREATION_DATE        VARCHAR     NOT NULL,
            REVIEW_ANSWER_TIMESTAMP     VARCHAR     NOT NULL
        )
    """,
    "olist_sellers_dataset.csv": """
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.olist_sellers_dataset (
            SELLER_ID                   VARCHAR     NOT NULL,   -- Primary Key
            SELLER_ZIP_CODE_PREFIX      VARCHAR     NOT NULL,
            SELLER_CITY                 VARCHAR     NOT NULL,
            SELLER_STATE                VARCHAR     NOT NULL
        )
    """,
    "olist_products_dataset.csv": """
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.olist_products_dataset (
            PRODUCT_ID                      VARCHAR     NOT NULL,   -- Primary Key
            PRODUCT_CATEGORY_NAME           VARCHAR,                -- 610 nulls expected
            PRODUCT_NAME_LENGHT             VARCHAR,                -- 610 nulls expected (sic — preserving source spelling)
            PRODUCT_DESCRIPTION_LENGHT      VARCHAR,                -- 610 nulls expected (sic — preserving source spelling)
            PRODUCT_PHOTOS_QTY              VARCHAR,                -- 610 nulls expected
            PRODUCT_WEIGHT_G                VARCHAR,                -- 2 nulls expected
            PRODUCT_LENGTH_CM               VARCHAR,                -- 2 nulls expected
            PRODUCT_HEIGHT_CM               VARCHAR,                -- 2 nulls expected
            PRODUCT_WIDTH_CM                VARCHAR                 -- 2 nulls expected
        )
    """,
    "product_category_name_translation.csv": """
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.product_category_name_translation (
            PRODUCT_CATEGORY_NAME           VARCHAR     NOT NULL,
            PRODUCT_CATEGORY_NAME_ENGLISH   VARCHAR     NOT NULL
        )
    """,
    "olist_geolocation_dataset.csv": """
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.olist_geolocation_dataset (
            GEOLOCATION_ZIP_CODE_PREFIX     VARCHAR     NOT NULL,
            GEOLOCATION_LAT                 VARCHAR     NOT NULL,
            GEOLOCATION_LNG                 VARCHAR     NOT NULL,
            GEOLOCATION_CITY                VARCHAR     NOT NULL,
            GEOLOCATION_STATE               VARCHAR     NOT NULL
        )
    """,
}
