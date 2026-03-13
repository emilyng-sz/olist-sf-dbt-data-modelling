with source as (

    select * from {{ source('raw', 'olist_products_dataset') }}

),

renamed as (

    select
        product_id,
        product_category_name,
        -- fix typos in source column names
        cast(product_name_lenght as int)            as product_name_length,
        cast(product_description_lenght as int)     as product_description_length,
        cast(product_photos_qty as int)             as product_photos_qty,
        cast(product_weight_g as float)             as product_weight_g,
        cast(product_length_cm as float)            as product_length_cm,
        cast(product_height_cm as float)            as product_height_cm,
        cast(product_width_cm as float)             as product_width_cm,

        -- metadata
        {{ metadata_columns() }}

    from source

)

select * from renamed
