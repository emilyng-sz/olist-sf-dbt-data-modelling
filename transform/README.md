# dbt models
This folder contains the code related to dbt transformation

## Folder Structure
- Transformation SQL files (one for each table) are placed under each schema accordingly

```
transform/
├── models/
│   └── staging/
        ├── _sources.yml
        └── stg_olist_orders.sql
│   ├── intermediate/     # int_ models (joins, business logic)
│       └── _int_models.yml       # intermediate model tests go here
│   ├── marts/            # fct_ and dim_ models (final layer)
│   └── profiles.yml          # sets the environment
├── tests/
├── macros/
├── seeds/
├── snapshots/
├── dbt_project.yml       # project configuration
├── packages.yml          # external packages
└── 
```

## How Data Flows to the Marts Layer

```
stg_olist_geolocation                 ─┐
stg_olist_sellers                     ─┼──► int_zip_codes ──► dim_zip_code
stg_olist_customers                   ─┘
stg_product_category_name_translation ─┐
stg_olist_products                    ─┘──► int_categories
```

## Prerequisites
- Ensure `dbt debug` works
- Ensure the Snowflake role used has `CREATE SCHEMA` privileges on the database

## Commands
```
dbt run                        # uses whatever `target: dev` is set to in profiles.yml
dbt run --target prod          # explicitly runs against prod
dbt test                       # compiles each (custom one-off) test from `tests/` folder into a SELECT statement and executes it in Snowflake (i.e. tables must first exist)

dbt parse --target dev         # validates YAML syntax and catches structural errors without executing anything in Snowflake

# Visualise the full DAG in the browser
dbt docs generate --target dev
dbt docs serve                 # visualise warehouse info on http://localhost:8080
```
`dbt run`
1. Scans the `models/` folder recursively for every .sql file (defined in `dbt_project.yml`)
2. Compiles each one
3. Executes them in dependency order
- Note: For target tables materialised as a view, `dbt run` essentially calls: 
```
create or replace view dev_alice_stg.stg_olist_orders as (
    select ... from raw.olist_orders_dataset
)
```
- Views are a virtual definition and does not have physical data nor storage costs associated
- Views are only executed upon read or query of the view
- Each `dbt run` WILL re-run and re-compute all models (unless set to incremental load)

## Environment control in dbt
- Commands were run in `dev` before `prd`
- target schema for each table is specified partially in `profiles.yml` and `_sources.yml`
    - `profiles.yml` sets the prefix in `schema` variable
    - `_source.yml` sets the suffix in `schema` variable
    - schema resolution: target.schema + "_" + config_schema
    - which prefix schema is read in `profiles.yml` is determined by the `--target` variable upon `dbt run`.
        - unspecified: `target` value taken

## Model files in dbt

- **CTEs**
    - used to represent one discrete transformation step or logic (e.g. cleaned, joined)
    - the final `SELECT` statement should be a simple step to emit the table. 
        - for `stg` and `int`, `SELECT *` is allowed
        - for final `mart` layer, columns should be explicit
    - are comma separated. A comma after CTE indicates parser to expect another CTE, instead of a `SELECT` statement.
- **Reference**: 
    - `{{ ref() }}` takes the model filename (without .sql) as its argument. dbt then resolves it to the fully qualified Snowflake path **internally**
    - schema prefixes inside `ref()` is therefore incorrect.

## dbt Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
