{% macro generate_attribute_columns(attribute_names, source_column='Attributes', table_alias=None) %}
    {# Generate column definitions for each attribute name #}
    {% set results = [] %}
    
    {% for attr_name in attribute_names %}
		 {% set col_ref = (table_alias ~ '.' if table_alias is not none else '') ~ 'Attributes' %}
	
         {% set col = "try_element_at(filter(" ~ col_ref ~ ", x -> x.Name = '" ~ attr_name ~ "'), 1).Value AS " ~ attr_name %}
        {% do results.append(col) %}
    {% endfor %}
    
    {% set sql = results | join(',\n    ') %}
    {{ log(sql, info=True) }}
    {{ return(sql) }}
{% endmacro %}