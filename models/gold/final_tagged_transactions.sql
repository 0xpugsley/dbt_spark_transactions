{{ config(materialized='table') }}

-- Get the distinct tags dynamically
{% set tag_query %}
    SELECT DISTINCT tag
    FROM {{ ref('tagged_transactions') }}
{% endset %}

{% set raw_tags = run_query(tag_query) | map(attribute='tag') | list %}
{% set tags = raw_tags | reject('none') | reject('eq', None) | list %}

SELECT
    txn_id,
    {% for tag in tags %}
    MAX(CASE WHEN tag = '{{ tag }}' THEN TRUE ELSE FALSE END) AS {{ tag | lower }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ ref('tagged_transactions') }}
GROUP BY txn_id
ORDER BY txn_id