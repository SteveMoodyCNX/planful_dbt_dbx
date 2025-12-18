{{
  config(
    materialized='table',
    schema='db_gold',
    comment='Gold dimension table for Planful Scenarios'
  )
}}

WITH scenarios_from_data AS (
    -- Get distinct scenario/tenant combinations that actually exist in the data
    SELECT DISTINCT
        Scenario,
        _tenant_name
    FROM {{ ref('planful_gl_data') }}
),

with_config AS (
    -- Join to config table for reporting metadata
    SELECT
        SHA2(CONCAT_WS('|', 
            COALESCE(s.Scenario, ''),
            COALESCE(s._tenant_name, '')
        ), 256) AS scenario_key,
        s.Scenario AS scenario_name,
        s._tenant_name AS tenant_name,
        c.report_column_name,
        c.priority AS sort_order,
        c.is_active
    FROM scenarios_from_data s
    LEFT JOIN db_utility.planful_api_load_config c
        ON s._tenant_name = c.tenant
        AND s.Scenario = c.scenario_type
)

SELECT
    scenario_key,
    scenario_name,
    tenant_name,
    report_column_name,
    sort_order,
    COALESCE(is_active, false) AS is_active
FROM with_config
