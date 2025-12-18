{{
  config(
    materialized='table',
    schema='db_gold',
    comment='Gold dimension table for Planful locations'
  )
}}

WITH source AS (
    SELECT
        s._location_key,
        s.Location_Leaf_ID,
        s.Location_Description,
        s.MemberType,
        s.ActiveStatus,
        s.ParentCode,
        s.ParentId,
        s.Location_Leaf_Line_ID,
        -- Exploded attributes
        s.LOC_FUSION,
        s.SPAN_OF_CONTROL_SUB_COUNTRY,
        -- Hierarchy
        s.RollupLevels,
        s.current_level,
        s._tenant_name
    FROM {{ ref('planful_location') }} s
    WHERE s._is_deleted = false
),

-- Deduplicate: prefer Consolidation tenant, fallback to Transaction
deduped AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _location_key 
        ORDER BY CASE WHEN _tenant_name = 'Consolidation' THEN 1 ELSE 2 END
    ) = 1
),

-- Self-join to get parent surrogate key
with_parent_key AS (
    SELECT
        d._location_key AS location_key,
        p._location_key AS parent_location_key,
        d.Location_Leaf_ID AS location_id,
        d.Location_Description AS location_name,
        d.MemberType AS member_type,
        d.ActiveStatus AS active_status,
        d.ParentCode AS parent_code,
        -- Exploded attributes
        d.LOC_FUSION AS loc_fusion,
        d.SPAN_OF_CONTROL_SUB_COUNTRY AS span_of_control_sub_country,
        -- Hierarchy
        d.RollupLevels AS rollup_levels,
        d.current_level
    FROM deduped d
    LEFT JOIN deduped p
        ON d.ParentId = p.Location_Leaf_Line_ID
)

SELECT * FROM with_parent_key
