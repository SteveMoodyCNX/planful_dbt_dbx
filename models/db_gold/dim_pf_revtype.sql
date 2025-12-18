{{
  config(
    materialized='table',
    schema='db_gold',
    comment='Gold dimension table for Planful Revenue Type'
  )
}}

WITH source AS (
    SELECT
        s._revtype_key,
        s.Revenue_Type_Leaf_ID,
        s.Revenue_Type,
        s.MemberType,
        s.ActiveStatus,
        s.ParentCode,
        s.ParentId,
        s.Revenue_Type_Leaf_Line_ID,
        -- Hierarchy
        s.RollupLevels,
        s.current_level,
        s._tenant_name
    FROM {{ ref('planful_revtype') }} s
    WHERE s._is_deleted = false
),

-- Deduplicate: prefer Consolidation tenant, fallback to Transaction
deduped AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _revtype_key 
        ORDER BY CASE WHEN _tenant_name = 'Consolidation' THEN 1 ELSE 2 END
    ) = 1
),

-- Self-join to get parent surrogate key
with_parent_key AS (
    SELECT
        d._revtype_key AS revtype_key,
        p._revtype_key AS parent_revtype_key,
        d.Revenue_Type_Leaf_ID AS revtype_id,
        d.Revenue_Type AS revtype_name,
        d.MemberType AS member_type,
        d.ActiveStatus AS active_status,
        d.ParentCode AS parent_code,
        -- Hierarchy
        d.RollupLevels AS rollup_levels,
        d.current_level
    FROM deduped d
    LEFT JOIN deduped p
        ON d.ParentId = p.Revenue_Type_Leaf_Line_ID
)

SELECT * FROM with_parent_key
