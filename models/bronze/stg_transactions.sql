{{
    config(
        materialized="table",
        partition_by=["txn_date"],
        pre_hook=[
            "SET hive.exec.dynamic.partition.mode=nonstrict",
            "{{ log('Setting nonstrict mode', info=True) }}",
        ],
    )
}}

select
    txn_id,
    txn_date,
    amount,
    description,
    code,
    country_code,
    counterparty_number,
    counterparty_name
from {{ source("raw", "raw_transactions") }}
