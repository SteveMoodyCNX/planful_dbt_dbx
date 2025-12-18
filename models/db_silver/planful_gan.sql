{{
  config(
    materialized='incremental',
    unique_key=['_gan_key','_tenant_name'],
    incremental_strategy='merge',
	on_schema_change = 'sync_all_columns',
    options = {"mergeSchema": "true"},
    schema='db_silver',
    comment='Silver table for Planful GAN hierarchy with CDC tracking and flattened rollup levels'
     )
}}

WITH source_data_raw AS (
	SELECT 
		MemberId AS `GAN_Leaf_Line_ID`,
		Code AS `GAN_Leaf_ID`,
		Name AS `GAN`,
		SHA2(CONCAT_WS('|', 
		    COALESCE(Code, ''),
		    COALESCE(Name, '')
		), 256) AS `_gan_key`,
		RollupOperator,
		MemberType,
		ActiveStatus,
		Sort,
		ParentId,
		ParentCode,
		Attributes,
		RollupLevels,
		COALESCE(array_max(transform(t.RollupLevels, x -> CAST(x.Level AS INT))), 0) + 1 AS current_level,
		{{ generate_rollup_columns('GAN', 1, table_alias='t', include_hash=False) }},
		_source_loaded_at,
		_source_file,
		_tenant_name
FROM {{ source('db_bronze', 'planful_gan') }} t 
),

source_data AS (
  SELECT * EXCEPT(Sort)
  FROM source_data_raw
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _gan_key, _tenant_name ORDER BY Sort ASC) = 1
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
      COALESCE(CAST(GAN_Leaf_Line_ID AS STRING), ''),
      COALESCE(RollupOperator, ''),
      COALESCE(MemberType, ''),
      COALESCE(ActiveStatus, ''),
      COALESCE(CAST(ParentId AS STRING), ''),
      COALESCE(ParentCode, ''),
      COALESCE(CAST(Attributes AS STRING), ''),
      COALESCE(CAST(RollupLevels AS STRING), ''),
      COALESCE(CAST(current_level AS STRING), '')
    )) AS _record_hash
  FROM source_data 
)

{% if is_incremental() %}
,


-- Identify records to soft delete (exist in Silver but not in Bronze)
records_to_delete AS (
  SELECT 
    t.GAN_Leaf_Line_ID,
    t.GAN_Leaf_ID,
    t.GAN,
    t._gan_key,
    t.RollupOperator,
    t.MemberType,
    t.ActiveStatus,
    t.ParentId,
    t.ParentCode,
    t.Attributes,
    t.RollupLevels,
    t.current_level,
    {{ generate_rollup_columns('GAN', 1, table_alias='t', include_hash=False) }},
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
    ON t._gan_key = s._gan_key
    AND t._tenant_name = s._tenant_name
  WHERE s._gan_key IS NULL
    AND t._is_deleted = FALSE
),


-- Determine if records are updates or inserts
source_with_change_type AS (
  SELECT 
    s.*,
    CASE 
      WHEN t._gan_key IS NULL THEN 'INSERT'
      WHEN s._record_hash != t._record_hash THEN 'UPDATE'
      ELSE 'NO_CHANGE'
    END AS actual_change_type
  FROM source_with_metadata s
  LEFT JOIN {{ this }} t 
    ON s._gan_key = t._gan_key 
    AND s._tenant_name = t._tenant_name
    AND t._is_deleted = FALSE
),

final_source AS (
  SELECT 
    t.GAN_Leaf_Line_ID,
    t.GAN_Leaf_ID,
    t.GAN,
    t._gan_key,
    t.RollupOperator,
    t.MemberType,
    t.ActiveStatus,
    t.ParentId,
    t.ParentCode,
    t.Attributes,
    t.RollupLevels,
    t.current_level,
    {{ generate_rollup_columns('GAN', 1, table_alias='t', include_hash=False) }},
    t._source_loaded_at,
    t._source_file,
    t._tenant_name,
    t._processed_at,
    t._is_deleted,
    t._deleted_at,
    t.actual_change_type AS _change_type,
    t._record_hash
  FROM source_with_change_type t
  WHERE actual_change_type != 'NO_CHANGE'  -- Only merge changed/new records
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
