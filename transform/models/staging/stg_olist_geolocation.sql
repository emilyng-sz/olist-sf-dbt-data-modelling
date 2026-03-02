with source as (

    select * from {{ source('raw', 'olist_geolocation_dataset') }}

),

renamed as (

    select
        cast(geolocation_zip_code_prefix as int)    as geolocation_zip_code_prefix,
        cast(geolocation_lat as float)              as geolocation_lat,
        cast(geolocation_lng as float)              as geolocation_lng,
        geolocation_city,
        geolocation_state,

        -- metadata
        {{ metadata_columns() }}

    from source

)

select * from renamed
