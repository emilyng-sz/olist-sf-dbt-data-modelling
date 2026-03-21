/*
    dim_products.sql
    ----------------
    PURPOSE:
        Product dimension. SCD Type 1 — product attributes (category, dimensions,
        weight) are overwritten on change; no historical versioning required for
        standard analytical use cases.

    DERIVED METRIC:
        volume_cm3 (L × W × H): deterministic function of source columns
        reused across delivery cost modelling and logistics analysis. 
*/

with products as (

    select * from {{ ref('int_products') }}

),

final as (

    select
        product_sk,
        product_id,
        category_sk,                        -- FK → dim_category

        product_name_length,
        product_description_length,
        product_photos_qty,

        -- Physical attributes — (converted to integers in intermediate)
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,

        -- Derived metric
        product_length_cm
            * product_height_cm
            * product_width_cm              as volume_cm3,

        -- metadata
        {{ metadata_columns() }}

    from products

)

select * from final
