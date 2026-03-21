/*
    fct_reviews.sql
    ---------------
    GRAIN: One row per review

    NOTE: review_sk is surrogate key of the review_id and order_id composite natural key.
*/

with reviews as (

    select * from {{ ref('int_reviews') }}

),

final as (

    select
        review_sk,
        review_id,                      -- natural key (not unique alone)
        order_id,                       -- degenerate dimension (FK → dim_orders)

        review_score,
        is_valid_score,
        review_sentiment,               -- negative / neutral / positive / unknown
        has_comment,

        review_comment_title,
        review_comment_message,

        review_creation_date,
        review_answer_timestamp,
        hours_to_respond,               -- response lag in hours

        -- metadata
        {{ metadata_columns() }}

    from reviews

)

select * from final
