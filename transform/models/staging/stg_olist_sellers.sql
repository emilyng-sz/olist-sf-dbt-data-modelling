with source as (

    select * from {{ source('raw', 'olist_sellers_dataset') }}

),

renamed as (

    select
        seller_id,
        cast(seller_zip_code_prefix as int)     as seller_zip_code_prefix,
        seller_city,
        upper(seller_state)                     as seller_state,

        -- metadata
        {{ metadata_columns() }}

    from source

)

select * from renamed
