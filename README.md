# Olist pipeline 
This repository contains an end-to-end data ingestion and aims to mimic industry-level data engineering concepts.

- Source: local csv files
- Data Ingestion: snowflake-connector-python. In Production, tools such as Fivetran or Airbyte can be used instead.
- Data Warehouse: **Snowflake**
- Data Transformation: **dbt**


## Dataset and Business Objectives
The dataset used contains ~100k anonymised Brazilian e-commerce transactions from Olist's marketplace platform (2016–2018), spanning the full order lifecycle including orders, customers, products, sellers, payments, and reviews. Business applications include customer segmentation, delivery performance analysis and seller benchmarking that can drive improvements in marketplace experience.

## Data Exploration
- Refer to `EDA_pandas.ipynb` for data exploration
- Refer to `Olist Raw Data Schema.csv` for the Data Schema and consolidation of row level findings from `EDA_pandas.ipynb`

### Dataset Limitations
Olist dataset does not track changing dimensions. Particularly, the implementation of `orders` table has updates taking in-place of the table (e.g. delivered status modifies the row in-place by adding deleivered_datetime and overwriting the existing status). While there are beneits of this implementation design, my ingestion therefore focuses on a one-time bulk ingestion. 

## Data Architecture Design Decisions

### Snowflake

#### Cost Management
- Cost is incured via:
  - Compute e.g. when Warehouse is running: XS warehouse costs 1 credit/hr Small is 2 credits/hr etc.
    - Solution: Warehouse will auto-suspend after 60 seconds of inactivity
  - Storage charged monthly at ~$23/TB/month on-demand
    - This is relatively cheap

#### Data Management
- dbt provides three materialisation options
  1. **View**: dbt runs CREATE OR REPLACE VIEW. No data is stored, only the query logic.
  2. **Table**: dbt runs CREATE OR REPLACE TABLE AS SELECT. Data is physically written to Snowflake storage.
  3. **Ephermeral**: dbt does not create anything in Snowflake. The SQL query logic is injected as a CTE to downstream dbt models. Query exists only in compiled SQL, not in Snowflake.
    - Ephermeral is good if int table is simple
    - Since ephermeral data cannot be queried, test failures are harder to trace
- Staging is materialised as View, as it is a 1-1 mapping to raw data, one-time loaded to Snowflake
- Intermediate is materialised as View by default, but certain complex tables are materialised as Tables.
- Marts are Tables for efficient querying

## dbt
This is initialised in the `transform` folder with folder structure

```
transform/
├── models/
│   ├── staging/          # stg_ models
│   ├── intermediate/     # int_ models (joins, business logic)
│   └── marts/            # fct_ and dim_ models (final layer)
├── tests/
├── macros/
├── seeds/
├── snapshots/
├── dbt_project.yml       # project configuration
└── packages.yml          # external packages
```

### dbt Implementation Limitations
- There is only one developer (myself) on this project, and there is therefore only one dev environment
- The same role that ingestions data to the raw schema is used to transform the data

## Data Pipeline

1. Ingestion to `raw` schema:
  - Snowflake Python connector in `ingestion_raw.py`. In production, this can be replaced with ingestion tools such as Fivetran or Airbyte to ensure immutability
  - Note:
    - raw tables do not have any transformation and is a 1-1 mapping of the source data
    - All columns are ingested as varchar to ensure COPY INTO does not fail due to a schema mismatch
    - Non null columns are not enforced here

2. `stg` models are still a 1-1 mapping of `raw` tables with only data integrity based cleaning rules applied 
  - Transformations in this step should be **reversible** and **not requiring specific business judgement** 
    - E.g. 1. Upper casing of `state` column is reversible and matches Brazilian postal authority standards. The casing does not contain business meaning pertaining to Olist.
    - E.g. 2. Cleaning `city` column to match equivalence across tables is non-reversible and is a business decision about equivalence, i.e. 'são paulo' and 'sao paulo' being the same entity serves a specific business purpose and does NOT belong in staging.
  - No PII masking required for this dataset

3. Build intermediate tables
  - Business logic is applied here:
      - Add "Uncategorised" as a new category
      - Datetime validity (e.g. based on order_status) and order_status validility
  - Each intermediate table has a clear purpose
  - Some tables are clean enough that do not require this intermediate layer
  - May have normalisation of tables

4. Mart layer
  - Build dimensions before facts, due to foreign key referential dependencies
  - Data Marts:
    a. Products Table
    b. Customer x Products Table
  - ERD:
![alt text](image.png)

# How to run
1. Venv
2. pip install requirements.txt
3. dbt deps

### Business Use Cases
- Products Table: 
    - for logistics planning and prediction of product volumes
    - product_id is PK
    - Answers: 
        - Which categories have highest sales volume and during which month of the year? 
        - Which categories have highest revenue and during which month of the year? 
        - Are there specific peak volumes of categories that customers expect to be delievered by a certain date (e.g. gifts before christmas)
        - Which categories have the highest / lowest price:freight_value, how can the freight_value be optimised?
- Customer x Products Table:
    - Buying patterns of ONE customer over time
    - Answers: 
        - How frequently do customers buy an item or take to buy their next item? 
        - Does the amount spent affect it? (Can use Z-scores to determine their normal behaviour and how much it deviates to determine)
        - What is the cancellation rate for this customer, and at which stage does cancellation happen most often

### Other Potential Business Use Cases
- How customer satisfaction is affected (e.g. by shipment delivery, installment options etc)
    - Limitations: no point-in-time data to showcase e.g. funnel of shipments.
- How sellers price their products and if it changes (type 2 SCD for sellers and product price)


# Citations
Olist, and André Sionek. (2018). Brazilian E-Commerce Public Dataset by Olist [Data set]. Kaggle. https://doi.org/10.34740/KAGGLE/DSV/195341 