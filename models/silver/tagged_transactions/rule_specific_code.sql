{{ config(materialized="ephemeral") }}

select txn_id, 'SpecialTransaction' as tag, 2 as tag_id
from {{ ref("stg_transactions") }}
where code in ('TXN001', 'PAY001')
