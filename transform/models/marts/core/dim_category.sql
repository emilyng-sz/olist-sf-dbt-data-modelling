/*
    dim_category.sql
    ----------------
    PURPOSE:
        Product category dimension. SCD Type 1. Provides both the original
        Portuguese category names and their English translations for
        bilingual reporting. FK target from dim_products.category_sk.
*/

with categories as (

    select * from {{ ref('int_categories') }}

),

final as (

    select
        category_sk,
        category_name,              -- Portuguese (source natural key)
        category_name_english,      -- English translation for reporting

        -- metadata
        {{ metadata_columns() }}

    from categories

)

select * from final
