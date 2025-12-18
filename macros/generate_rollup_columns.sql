{% macro generate_rollup_columns(table_type, max_levels, keys=None, alias_map=None, table_alias=None, include_hash=True, hash_keys=None, hash_only=False) %}
  {# Default keys if none provided #}
  {% if keys is none %}
    {% set keys = var('rollup_keys', ['Code', 'Name', 'Id']) %}
  {% endif %}

  {# Default hash_keys to Code and Name if not provided #}
  {% if hash_keys is none %}
    {% set hash_keys = ['Code', 'Name'] %}
  {% endif %}

  {% set results = [] %}

  {# Alias map from vars if not provided #}
  {% if alias_map is none %}
    {% set rollup_aliases = var('rollup_aliases', {}) %}
    {% set alias_map = rollup_aliases.get(table_type, {}) %}
  {% endif %}

  {% set rollup_ref = (table_alias ~ '.' if table_alias is not none else '') ~ 'RollupLevels' %}
  {% set filter_expr = "try_element_at(FILTER(" ~ rollup_ref ~ ", x -> x.Level = " %}

  {% for level in range(1, max_levels + 1) %}
    {# Generate columns for each key (unless hash_only) #}
    {% if not hash_only %}
      {% for key in keys %}
        {% set alias_suffix = alias_map.get(key, key) %}
        {% set col = filter_expr ~ level ~ "), 1)." ~ key ~ " AS " ~ table_type ~ "_Level_" ~ level ~ "_" ~ alias_suffix %}
        {% do results.append(col) %}
      {% endfor %}
    {% endif %}
    
    {# Generate hash column for this level if enabled #}
    {% if include_hash %}
      {% set hash_parts = [] %}
      {% for hash_key in hash_keys %}
        {% do hash_parts.append("COALESCE(" ~ filter_expr ~ level ~ "), 1)." ~ hash_key ~ ", '')") %}
      {% endfor %}
      {% set null_check = "'" ~ ("|" * (hash_keys | length - 1)) ~ "'" %}
      {% set hash_col %}
SHA2(
  NULLIF(CONCAT_WS('|',
    {{ hash_parts | join(',\n    ') }}
  ), {{ null_check }}),
256) AS {{ table_type }}_Level_{{ level }}_Hash
      {%- endset %}
      {% do results.append(hash_col) %}
    {% endif %}
  {% endfor %}

  {% set sql = results | join(',\n ') %}
  {{ log(sql, info=True) }}
  {{ return(sql) }}
{% endmacro %}