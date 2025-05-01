{{ config(materialized="ephemeral") }}

select txn_id, 'RetailPurchase' as tag, 3 as tag_id
from {{ ref("stg_transactions") }}
where
    regexp_like(lower(description), '\\b(store|market)\\b')
    and counterparty_name like '%Market%'
