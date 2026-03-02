with source as (

    select * from {{ source('raw', 'olist_customers_dataset') }}

),

renamed as (

    select
        customer_id,
        customer_unique_id,
        cast(customer_zip_code_prefix as int) as customer_zip_code_prefix,
        customer_city,
        upper(customer_state)                 as customer_state,

        -- metadata
        {{ metadata_columns() }}

    from source

)

select * from renamed
