with source as (

    select * from {{ source('raw', 'olist_order_payments_dataset') }}

),

renamed as (

    select
        order_id,
        cast(payment_sequential as int) as payment_sequential,
        payment_type,
        cast(payment_installments as int) as payment_installments,
        cast(payment_value as float)    as payment_value,

        -- metadata
        {{ metadata_columns() }}

    from source

)

select * from renamed
