-- marts/core/dim_zip_code.sql
--
-- Geographic dimension: one row per zip code prefix.
-- FK target for dim_customers and dim_sellers.
-- See int_zip_code for build logic and source priority rules.

with zip_codes as (

    select * from {{ ref('int_zip_code') }}

)

select
    -- Columns prefixed with 'geolocation_' to match the ERD and avoid
    -- ambiguity when this dimension is joined in wide mart queries
    zip_code_prefix         as geolocation_zip_code_prefix,
    city                    as geolocation_city,
    state                   as geolocation_state,

    -- metadata
    {{ metadata_columns() }}

from zip_codes
