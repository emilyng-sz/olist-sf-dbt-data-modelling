/*
    int_zip_codes.sql
    ---------------------------
    PURPOSE:
        Builds the most complete possible zip code dimension by unioning
        zip code + city/state from three sources: geolocation (primary),
        sellers (secondary), and customers (tertiary).

        A zip code that appears only in the sellers dataset but not in
        geolocation would otherwise be missing from dim_zip_code, breaking
        the FK relationship from dim_sellers.

    SOURCE PRIORITY:
        1. geolocation  — dedicated geographic reference table, most reliable
        2. sellers      — small, clean, business-relevant addresses
        3. customers    — largest dataset but noisier address data

    PATTERN: UNION ALL → deduplicate by priority
        UNION ALL combines all rows from all sources.
        ROW_NUMBER() partitioned by zip code then ordered by source_priority
        keeps exactly one row per zip code — always from the highest-priority
        source that has it.
*/

with geolocation as (

    select
        geolocation_zip_code_prefix     as zip_code_prefix,
        geolocation_city                as city,
        geolocation_state               as state,
        1                               as source_priority   -- most trusted
    from {{ ref('stg_olist_geolocation') }}

),

sellers as (

    select
        seller_zip_code_prefix          as zip_code_prefix,
        seller_city                     as city,
        seller_state                    as state,
        2                               as source_priority
    from {{ ref('stg_olist_sellers') }}

),

customers as (

    select
        customer_zip_code_prefix        as zip_code_prefix,
        customer_city                   as city,
        customer_state                  as state,
        3                               as source_priority
    from {{ ref('stg_olist_customers') }}

),

-- Combine all three sources into one pool of zip code + city/state candidates
all_sources as (

    select * from geolocation
    union all
    select * from sellers
    union all
    select * from customers

),

-- Clean city names consistently across all sources before deduplication
-- so "são paulo" from geolocation and "sao paulo" from customers
-- don't produce two different rows for the same zip code
cleaned as (

    select
        zip_code_prefix,
        -- initcap(regexp_replace(lower(city), '[àáâãä]', 'a')) as city,
        initcap(
            translate(
                lower(city),
                'àáâãäèéêëìíîïòóôõöùúûüýÿñç',
                'aaaааeeeeiiiioooouuuuyyns'
                )
         ) as city,
        state,
        source_priority,
        -- check if state is a capitalised 2-letter string
        regexp_like(state, '^[A-Z]{2}$')   as is_valid_state,

    from all_sources

),

-- Business rule: drop records with invalid state codes
-- Explicit filter with a clear comment so the row loss is never invisible
filtered as (

    select * from cleaned
    where is_valid_state = true
    -- Rows dropped here: state is not exactly two uppercase alphabetic characters
    -- These represent malformed source data with no valid geographic resolution

),

-- Keep exactly one row per zip code, preferring the highest-priority source
deduplicated as (

    select
        zip_code_prefix,
        city,
        state,
        source_priority
    from filtered

    qualify row_number() over (
        partition by zip_code_prefix
        order by source_priority asc      -- 1 wins over 2 wins over 3
    ) = 1

)

select
    zip_code_prefix,
    city,
    state,

    -- metadata
    {{ metadata_columns() }}

from deduplicated
