with source as (

    select * from {{ source('raw', 'product_category_name_translation') }}

),

renamed as (

    select
        product_category_name,
        product_category_name_english,

        -- metadata
        {{ metadata_columns() }}

    from source

)

select * from renamed
