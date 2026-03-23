with source as (

    select * from {{ source('raw', 'olist_order_items_dataset') }}

),

renamed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_date,
        cast(price as float) as price,
        cast(freight_value as float) as freight_value,

        -- metadata
        {{ metadata_columns() }}

    from source

)

select * from renamed
