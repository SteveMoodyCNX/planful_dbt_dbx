-- macros/pivot_scenarios.sql

{% macro pivot_scenarios(value_column='mtd_amount', agg='sum') %}

{% set scenario_query %}
    select 
        tenant,
        scenario_type,
        report_column_name
    from db_utility.planful_api_load_config
    where is_active = true
      and report_column_name is not null
    order by priority
{% endset %}

{% set results = run_query(scenario_query) %}

{% if execute %}
    {% for row in results %}
    {{ agg }}(case 
        when d_scen._tenant_name = '{{ row.tenant }}' 
         and d_scen.Scenario = '{{ row.scenario_type }}' 
        then f.{{ value_column }} 
    end) as {{ row.report_column_name }}
    {%- if not loop.last %},{% endif %}
    {% endfor %}
{% endif %}

{% endmacro %}