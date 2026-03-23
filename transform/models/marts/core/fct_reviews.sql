/*
    fct_reviews.sql
    ---------------
    GRAIN: One row per review

    NOTE: review_sk is surrogate key of the review_id and order_id composite natural key.
*/

with reviews as (

    select * from {{ ref('int_reviews') }}

),

orders as (

    select order_id, order_sk from {{ ref('int_orders') }}

),

final as (

    select
        r.review_sk,
        o.order_sk,                     -- FK → dim_orders (not all orders have reviews)

        r.review_score,
        r.is_valid_score,
        r.review_sentiment,               -- negative / neutral / positive / unknown
        r.has_comment,

        r.review_comment_title,
        r.review_comment_message,

        r.review_creation_date,
        
        cast(
            to_varchar(review_creation_date, 'YYYYMMDD')
        as int)                                         as review_creation_date_sk,

        r.review_answer_timestamp,
        r.hours_to_respond,               -- response lag in hours

        -- metadata
        {{ metadata_columns() }}

    from reviews r
    left join orders o
        on r.order_id = o.order_id

)

select * from final
