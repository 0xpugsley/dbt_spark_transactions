{% macro get_column_name_having_prefix(prefix) %}

    {% set result = [] %}

    {% set relation = ref("final_tagged_transactions_pivot") %}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% for column in columns %}
        {% if column.name != "txn_id" and (
            prefix == "" or column.name.startswith(prefix)
        ) %}
            {% do result.append(column.name) %}
        {% endif %}
    {% endfor %}

    {{ return(result) }}

{% endmacro %}
