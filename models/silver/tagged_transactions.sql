{{ config(materialized="table") }}

with
    all_tags as (
        select *
        from {{ ref("rule_description_food") }}
        union all
        select *
        from {{ ref("rule_specific_code") }}
        union all
        select *
        from {{ ref("rule_counterparty") }}
    )

select t.*, at.tag, at.tag_id
from {{ ref("stg_transactions") }} t
left join all_tags at on t.txn_id = at.txn_id
where at.tag is not null
