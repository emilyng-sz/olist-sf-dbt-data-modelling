/*
    int_products.sql
    ----------------
    PURPOSE:
        Joins products to int_categories to resolve category_sk, which dim_products
        requires as a foreign key to dim_category. Also fixes the source column
        name typos (lenght → length), and handles the 610 null-category products 
        consistently with int_categories.

    DATA QUALITY NOTE:
        610 products have null across all descriptive fields (category, name length,
        description length, photos qty). These appear to be data entry gaps in the
        source system. They are retained with nulls visible — dropping them would
        silently remove valid product_ids that appear in order items.

    KEY DECISIONS:
        - product_sk is generated from product_id. This is a stable 1:1 mapping.
        - Dimensional measurements (weight, dimensions) are kept as floats.
          Rounding to integers as shown in dbdiagram is done here to match the
          mart target schema — int is appropriate since fractional grams/cm are
          not meaningful for analytical purposes.
        - No city/state: product location is not stored on the product entity.
          Seller location is resolved via dim_sellers → dim_zip_code.
*/

with products as (

    select * from {{ ref('stg_olist_products') }}

),

categories as (

    select
        category_sk,
        category_name
    from {{ ref('int_categories') }}

),

joined as (

    select
        {{ dbt_utils.generate_surrogate_key(['products.product_id']) }}     as product_sk,

        products.product_id,

        -- Resolve category_sk: FK to dim_category
        -- Null-category products get the 'uncategorised' category_sk,
        -- consistent with how int_categories handles nulls
        categories.category_sk,

        -- Cast to int: fractional character counts are not meaningful
        cast(products.product_name_length as int)           as product_name_length,
        cast(products.product_description_length as int)    as product_description_length,
        cast(products.product_photos_qty as int)            as product_photos_qty,

        -- Cast to int: fractional grams/cm not analytically meaningful
        cast(products.product_weight_g as int)              as product_weight_g,
        cast(products.product_length_cm as int)             as product_length_cm,
        cast(products.product_height_cm as int)             as product_height_cm,
        cast(products.product_width_cm as int)              as product_width_cm,

        -- metadata
        {{ metadata_columns() }}

    from products
    left join categories
        on coalesce(products.product_category_name, '{{ var("uncategorised_label") }}')
            = categories.category_name

)

select * from joined