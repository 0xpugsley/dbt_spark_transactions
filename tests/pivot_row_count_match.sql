-- This test ensures the pivoted table has the same number of distinct transactions as
-- the source
-- Returns rows if there's a mismatch in counts
with
    source_count as (
        select count(distinct txn_id) as cnt from {{ ref("tagged_transactions") }}
    ),

    pivot_count as (
        select count(*) as cnt from {{ ref("final_tagged_transactions_pivot") }}
    )

select 'Count mismatch' as issue, s.cnt as source_count, p.cnt as pivot_count
from source_count s
cross join pivot_count p
where s.cnt != p.cnt
