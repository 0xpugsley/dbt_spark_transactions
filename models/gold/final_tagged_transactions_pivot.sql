{{ config(materialized='table') }}

-- Get the distinct tags dynamically
{% set tag_query %}
    SELECT DISTINCT tag
    FROM {{ ref('tagged_transactions') }}
{% endset %}

{% set raw_tags = run_query(tag_query) | map(attribute='tag') | list %}
{% set tags = raw_tags | reject('none') | reject('eq', None) | list %}

WITH base_data AS (
    SELECT DISTINCT
        txn_id,
        tag
    FROM {{ ref('tagged_transactions') }}
)

SELECT
    p.txn_id,
    {% for tag in tags %}
    COALESCE(p.`{{ tag | replace("'", "''") }}`, FALSE) AS `{{ tag | replace("'", "''") }}`{% if not loop.last %},{% endif %}
    {% endfor %}
FROM (
    SELECT * FROM base_data
    PIVOT (
        COUNT(tag) > 0
        FOR tag IN (
            {% for tag in tags %}
                '{{ tag | replace("'", "''") }}'{% if not loop.last %}, {% endif %}
            {% endfor %}
        )
    )
) p