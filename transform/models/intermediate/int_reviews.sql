/*
    int_reviews.sql
    ---------------
    PURPOSE:
        review_id is NOT unique in the source. Hence, composite key is generated.
        Surrogate key uses (review_id, order_id). Sentiment category is also 
        added for analytical convenience.

    DATA QUALITY NOTE:
        - Columns likely to be null are coalesced to empty string so downstream
          text analysis doesn't need to handle nulls.
        - has_comment flag makes it trivial to filter reviewed vs non-reviewed
          orders without inspecting text fields.

    KEY DECISIONS:
        - Sentiment bucketing (negative/neutral/positive) is applied here because
          it is a deterministic, universally agreed mapping of score → category.
        - review_score validated and flagged via column `is_valid_score`: must be 
          between 1 and 5 per business rules.
*/

with reviews as (

    select * from {{ ref('stg_olist_order_reviews') }}

),

transformed as (

    select
        -- Generate surrogate key from composite natural key (review_id + order_id)
        {{ dbt_utils.generate_surrogate_key(['review_id', 'order_id']) }}   as review_sk,

        review_id,
        order_id,
        review_score,

        -- Business rule: validate score is within expected range
        case
            when review_score between 1 and 5   then true
            else false
        end                                 as is_valid_score,

        -- Sentiment category: deterministic mapping, centralised here
        -- so every downstream consumer uses the same definition
        case
            when review_score in (1, 2)     then 'negative'
            when review_score = 3           then 'neutral'
            when review_score in (4, 5)     then 'positive'
            else 'unknown'
        end                                 as review_sentiment,

        -- Null-safe text fields: coalesce to empty string preserves
        -- ability to do len() / text analysis without null guards everywhere
        coalesce(review_comment_title, '')      as review_comment_title,
        coalesce(review_comment_message, '')    as review_comment_message,

        -- Convenience flag: did this customer leave a written comment?
        case
            when review_comment_message is not null
                and review_comment_message != '' then true
            else false
        end                                 as has_comment,

        review_creation_date,

        -- Date surrogate key
        cast(
            to_varchar(review_creation_date::date, 'YYYYMMDD')
        as int)                                                                  as review_creation_date_sk,

        review_answer_timestamp,

        -- Response lag: how quickly did the seller respond?
        datediff(
            'hour',
            review_creation_date,
            review_answer_timestamp
        )                                   as hours_to_respond,

        -- metadata
        {{ metadata_columns() }}

    from reviews

)

select * from transformed