-- marts/dim_zip_code.sql

with zip_codes as (

    select * from {{ ref('int_zip_code') }}

)

select
    -- columns are prefixed with 'geolocation' to avoid ambiguity in the mart layer
    zip_code_prefix         as geolocation_zip_code_prefix,
    city                    as geolocation_city,
    state                   as geolocation_state,

    -- metadata
    {{ metadata_columns() }}

from zip_codes