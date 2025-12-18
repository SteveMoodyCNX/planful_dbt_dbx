{% test struct_keys_exist(model, column_name, id_column=None) %}
with exploded as (
    select
        {% if id_column %}{{ id_column }},{% endif %}
        exploded_element.Name as name,
        exploded_element.Value as value
    from {{ model }}
    lateral view explode({{ column_name }}) as exploded_element
    where {{ column_name }} is not null
)
select *
from exploded
where name is null or value is null
{% endtest %}
