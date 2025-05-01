{{ config(materialized="view") }}

select txn_id, count(*) as tag_count
from {{ ref("final_tagged_transactions") }}
group by txn_id
having tag_count > 1
