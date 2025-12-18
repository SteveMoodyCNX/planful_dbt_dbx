{{
  config(
    materialized='incremental',
    unique_key=['Account_Leaf_ID', 'Company_Leaf_ID', 'Intercompany_Leaf_ID', 'Location_Leaf_ID', 'Revenue_Type_Leaf_ID', 'Department_Leaf_ID', 'MSA_Leaf_ID', 'GAN_Leaf_ID', 'FiscalYear', 'FiscalMonth', 'Scenario', 'Reporting', '_tenant_name'],
    incremental_strategy='merge',
    partition_by=['FiscalYear', 'FiscalMonth', 'Scenario', '_tenant_name'],
    incremental_predicates=[
      "DBT_INTERNAL_DEST.Scenario = DBT_INTERNAL_SOURCE.Scenario",
      "DBT_INTERNAL_DEST.FiscalYear = DBT_INTERNAL_SOURCE.FiscalYear",
      "DBT_INTERNAL_DEST.FiscalMonth = DBT_INTERNAL_SOURCE.FiscalMonth",
      "DBT_INTERNAL_DEST._tenant_name = DBT_INTERNAL_SOURCE._tenant_name"
    ],
    schema='db_silver',
    comment='Silver table for Planful GL actuals data with standardized dimensional FK columns'
  )
}}

WITH source_data AS (
  SELECT 
    Scenario,
    FiscalYear,
    FiscalMonth,
    Segment1 AS Account_Leaf_ID,
    Segment2 AS Company_Leaf_ID,
    Segment3 AS Intercompany_Leaf_ID,
    Segment4 AS Location_Leaf_ID,
    Segment5 AS Revenue_Type_Leaf_ID,
    Segment6 AS Department_Leaf_ID,
    Segment7 AS MSA_Leaf_ID,
    Segment8 AS GAN_Leaf_ID,
    Reporting,
    MonthName,
    MtdAmount,
    YtdAmount,
    QtdAmount,
    _source_loaded_at,
    _load_id,
    _source_file,
    _tenant_name
  FROM {{ source('db_bronze', 'planful_gl_data') }}
  
  {% if is_incremental() %}
  WHERE (Scenario, FiscalYear, FiscalMonth,  _tenant_name) IN (
    SELECT DISTINCT Scenario, FiscalYear, FiscalMonth,  _tenant_name
    FROM {{ source('db_bronze', 'planful_gl_data') }}
    WHERE _source_loaded_at > (
      SELECT COALESCE(MAX(_source_loaded_at), CAST('1900-01-01' AS TIMESTAMP))
      FROM {{ this }}
    )
  )
  {% endif %}
),

final AS (
  SELECT 
    Scenario,
    FiscalYear,
    FiscalMonth,
    Account_Leaf_ID,
    Company_Leaf_ID,
    Intercompany_Leaf_ID,
    Location_Leaf_ID,
    Revenue_Type_Leaf_ID,
    Department_Leaf_ID,
    MSA_Leaf_ID,
    GAN_Leaf_ID,
    Reporting,
    MonthName,
    MtdAmount,
    YtdAmount,
    QtdAmount,
    _source_loaded_at,
    _load_id,
    _source_file,
    _tenant_name,
    CURRENT_TIMESTAMP() AS _processed_at
  FROM source_data
)

SELECT * FROM final