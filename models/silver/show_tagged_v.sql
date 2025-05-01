{{ config(materialized="view") }}

select txn_id, tag_id, tag
from {{ ref("tagged_transactions") }}
order by txn_id
