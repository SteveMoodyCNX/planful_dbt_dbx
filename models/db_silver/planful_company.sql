{{
  config(
    materialized='incremental',
    unique_key=['_company_key','_tenant_name'],
    incremental_strategy='merge',
	on_schema_change = 'sync_all_columns',
    options = {"mergeSchema": "true"},
    schema='db_silver',
    comment='Silver table for Planful company hierarchy with CDC tracking and flattened rollup levels'
	)
}}

WITH source_data_raw AS (
  SELECT 
    MemberId AS `Company_Leaf_Line_ID`,
    code AS `Company_ID`,
    name AS `Company`,
    SHA2(CONCAT_WS('|', 
        COALESCE(code, ''),
        COALESCE(name, '')
    ), 256) AS `_company_key`,
    RollupOperator,
    MemberType,
    ActiveStatus,
    ParentId,
    CurrencyCode,
    CurrencyName,
    ParentCode,
    Attributes,
    {{ generate_attribute_columns(['Country','SITE','SITE_DESC']) }},
    RollupLevels,
    COALESCE(array_max(transform(RollupLevels, x -> CAST(x.Level AS INT))), 0) + 1 AS current_level,
    {{ generate_rollup_columns('Company', 8, include_hash=False) }},
    _source_loaded_at,
    _source_file,
    _tenant_name,
    Sort
  FROM {{ source('db_bronze', 'planful_company') }}
),

source_data AS (
  SELECT * EXCEPT(Sort)
  FROM source_data_raw
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _company_key, _tenant_name ORDER BY Sort ASC) = 1
),

source_with_metadata AS (
  SELECT 
    *,
    CURRENT_TIMESTAMP() AS _processed_at,
    FALSE AS _is_deleted,
    CAST(NULL AS TIMESTAMP) AS _deleted_at,
    'INSERT' AS _change_type,
    -- Hash non-key business columns to detect changes
    MD5(CONCAT_WS('||', 
      COALESCE(CAST(Company_Leaf_Line_ID AS STRING), ''),
      COALESCE(RollupOperator, ''),
      COALESCE(MemberType, ''),
      COALESCE(ActiveStatus, ''),
      COALESCE(CAST(ParentId AS STRING), ''),
      COALESCE(CurrencyCode, ''),
      COALESCE(CurrencyName, ''),
      COALESCE(ParentCode, ''),
      COALESCE(CAST(Attributes AS STRING), ''),
      COALESCE(CAST(RollupLevels AS STRING), ''),
      COALESCE(CAST(current_level AS STRING), '')
    )) AS _record_hash
  FROM source_data
)

{% if is_incremental() %}
,
records_to_delete AS (
  SELECT 
    t.Company_Leaf_Line_ID,
    t.Company_ID,
    t.Company,
    t._company_key,
    t.RollupOperator,
    t.MemberType,
    t.ActiveStatus,
    t.ParentId,
    t.CurrencyCode,
    t.CurrencyName,
    t.ParentCode,
    t.Attributes,
   {{ generate_attribute_columns(['Country','SITE','SITE_DESC'], table_alias='t') }},
    t.RollupLevels,
    t.current_level,
   {{ generate_rollup_columns('Company', 8, table_alias='t', include_hash=False)}},
    t._source_loaded_at,
    t._source_file,
    t._tenant_name,
    CURRENT_TIMESTAMP() AS _processed_at,
    TRUE AS _is_deleted,
    CURRENT_TIMESTAMP() AS _deleted_at,
    'DELETE' AS _change_type,
    t._record_hash
  FROM {{ this }} t
  LEFT JOIN source_data s 
    ON t._company_key = s._company_key
    AND t._tenant_name = s._tenant_name
  WHERE s._company_key IS NULL
    AND t._is_deleted = FALSE
),

source_with_change_type AS (
  SELECT 
    s.*,
    CASE 
      WHEN t._company_key IS NULL THEN 'INSERT'
      WHEN s._record_hash != t._record_hash THEN 'UPDATE'
      ELSE 'NO_CHANGE'
    END AS actual_change_type
  FROM source_with_metadata s
  LEFT JOIN {{ this }} t 
    ON s._company_key = t._company_key
    AND s._tenant_name = t._tenant_name	
    AND t._is_deleted = FALSE
),

final_source AS (
  SELECT 
    Company_Leaf_Line_ID,
    Company_ID,
    Company,
    _company_key,
    RollupOperator,
    MemberType,
    ActiveStatus,
    ParentId,
    CurrencyCode,
    CurrencyName,
    ParentCode,
    Attributes,
    {{ generate_attribute_columns(['Country','SITE','SITE_DESC']) }},
    RollupLevels,
    current_level,
     {{ generate_rollup_columns('Company', 8, include_hash=False) }},
    _source_loaded_at,
    _source_file,
    _tenant_name,
    _processed_at,
    _is_deleted,
    _deleted_at,
    actual_change_type AS _change_type,
    _record_hash
  FROM source_with_change_type
  WHERE actual_change_type != 'NO_CHANGE'
),

final AS (
  SELECT * FROM final_source
  UNION ALL
  SELECT * FROM records_to_delete
)

{% endif %}

SELECT * FROM 
{% if is_incremental() %}
final
{% else %}
source_with_metadata
{% endif %}
