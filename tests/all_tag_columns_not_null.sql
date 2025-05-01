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

    null_check_query as (
        select t.txn_id, c.column_name, true as has_null_value
        from {{ ref("final_tagged_transactions_pivot") }} t
        cross join tag_columns c
        where
            (
                -- Dynamically check each column for NULL values
                case
                    {% for col in adapter.get_columns_in_relation(
                        ref("final_tagged_transactions_pivot")
                    ) %}
                        {% if col.name != "txn_id" %}
                            when
                                c.column_name = '{{ col.name }}'
                                and t.{{ col.name }} is null
                            then true
                        {% endif %}
                    {% endfor %}
                    else false
                end
            )
    )

-- Return rows where we found NULL values
select txn_id, column_name as null_column
from null_check_query
where has_null_value
