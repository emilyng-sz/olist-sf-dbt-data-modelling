{% macro metadata_columns() %}
    convert_timezone('Asia/Singapore', current_timestamp())  as _updated_at_sgt,
{% endmacro %}