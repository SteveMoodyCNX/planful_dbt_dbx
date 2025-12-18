{% test no_duplicate_attribute_names(model, column_name, id_column) %}
with exploded as (
    select
        {{ id_column }},
        attr.Name as name
    from {{ model }}
    lateral view explode({{ column_name }}) exploded_table as attr
    where {{ column_name }} is not null
)
select {{ id_column }}, name, count(*) as cnt
from exploded
group by {{ id_column }}, name
having count(*) > 1
{% endtest %}
