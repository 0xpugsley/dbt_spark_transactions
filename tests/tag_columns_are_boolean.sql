-- This test ensures all tag columns contain only boolean values (TRUE/FALSE)
-- Returns failing rows if any non-boolean values are found
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

    non_boolean_check as (
        select t.txn_id, c.column_name, true as has_non_boolean_value
        from {{ ref("final_tagged_transactions_pivot") }} t
        cross join tag_columns c
        where
            (
                -- Check if any value is not TRUE and not FALSE (and not NULL)
                case
                    {% for col in adapter.get_columns_in_relation(
                        ref("final_tagged_transactions_pivot")
                    ) %}
                        {% if col.name != "txn_id" %}
                            when
                                c.column_name = '{{ col.name }}'
                                and t.{{ col.name }} is not null
                                and t.{{ col.name }} != true
                                and t.{{ col.name }} != false
                            then true
                        {% endif %}
                    {% endfor %}
                    else false
                end
            )
    )

-- Return rows where we found non-boolean values
select txn_id, column_name as non_boolean_column
from non_boolean_check
where has_non_boolean_value
