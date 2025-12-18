{% macro apply_table_tag() %}
{%- set tags_list = [] -%}
{%- for item in model.config.meta.items() -%}
    {% do tags_list.append(item | join('"="')) %}
{%- endfor -%}
{%- set tags_string -%}
   {{ '"' + tags_list | join('","') + '"' }}
{%- endset -%}
{%- set comment_query -%}
    alter table {{ this | lower }} set tags ({{ tags_string }})
{%- endset -%}
{%- if tags_list | length > 0 -%}
    {{ return(comment_query) }}
{%- endif -%}
{% endmacro %}