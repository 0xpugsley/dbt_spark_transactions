-- This test ensures there are no NULL values in any of the tag columns
-- Returns failing rows if any NULL values are found
with
    tag_columns as (
        select column_name
        from
            (
                {% set relation = ref("final_tagged_transactions_pivot") %}
                {% set all_columns = adapter.get_columns_in_relation(relation) %}

                {% for column in all_columns if column.name != 'txn_id' %}
                    select '{{ column.name }}' as column_name
                    {% if not loop.last %}
                        union all
                    {% endif %}
                {% endfor %}
            )
    ),

    null_checks as (
        {% set pivot_model = ref("final_tagged_transactions_pivot") %}

        select
            txn_id,
            {% for col in adapter.get_columns_in_relation(pivot_model) if col.name != 'txn_id' %}
                case
                    when {{ col.name }} is null then 1 else 0
                end as {{ col.name }}_has_null,
            {% endfor %}
            0 as placeholder
        from {{ pivot_model }}
    )

    {% set pivot_model = ref("final_tagged_transactions_pivot") %}
    {% set tag_columns = (
        adapter.get_columns_in_relation(pivot_model)
        | selectattr("name", "ne", "txn_id")
        | list
    ) %}

{% if tag_columns | length > 0 %}
    {% for col in tag_columns %}
        {% if loop.first %} select
        {% else %}
            union all
            select
            {% endif %} txn_id,
            '{{ col.name }}' as column_with_null
        from null_checks
        where {{ col.name }}_has_null = 1
    {% endfor %}
{% else %}
    -- No tag columns found, return empty result
    select null as txn_id, null as column_with_null where 1 = 0
{% endif %}
