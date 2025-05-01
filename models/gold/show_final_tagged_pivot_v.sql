{{ config(materialized="view") }}

-- Get the distinct tags dynamically
{% set tag_query %}
    SELECT DISTINCT tag
    FROM {{ ref('tagged_transactions') }}
{% endset %}

{% set raw_tags = run_query(tag_query) | map(attribute="tag") | list %}
{% set tags = raw_tags | reject("none") | reject("eq", None) | list %}

select
    txn_id,
    {% for tag in tags %} {{ tag }} {% if not loop.last %},{% endif %} {% endfor %}
from {{ ref("final_tagged_transactions_pivot") }}
order by txn_id
