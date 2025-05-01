{{ config(materialized="ephemeral") }}

select txn_id, 'FoodPurchase' as tag, 1 as tag_id
from {{ ref("stg_transactions") }}
where regexp_like(lower(description), '\\b(banana|apple|grocery)\\b')
