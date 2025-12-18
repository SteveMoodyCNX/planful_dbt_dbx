{{
  config(
    materialized='table',
    schema='db_gold',
    comment='Gold dimension table for Planful departments'
  )
}}

WITH source AS (
    SELECT
        s._department_key,
        s.Department_Leaf_ID,
        s.Department,
        s.MemberType,
        s.ActiveStatus,
        s.ParentCode,
        s.ParentId,
        s.Department_Leaf_Line_ID,
        -- Exploded attributes
        s.Corp_Region_Site,
        s.Business_unit,
        s.SVP_Subgroup,
        s.DPT_FUSION_ID,
        -- Hierarchy
        s.RollupLevels,
        s.current_level,
        s._tenant_name
    FROM {{ ref('planful_department') }} s
    WHERE s._is_deleted = false
),

-- Deduplicate: prefer Consolidation tenant, fallback to Transaction
deduped AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _department_key 
        ORDER BY CASE WHEN _tenant_name = 'Consolidation' THEN 1 ELSE 2 END
    ) = 1
),

-- Self-join to get parent surrogate key
with_parent_key AS (
    SELECT
        d._department_key AS department_key,
        p._department_key AS parent_department_key,
        d.Department_Leaf_ID AS department_id,
        d.Department AS department_name,
        d.MemberType AS member_type,
        d.ActiveStatus AS active_status,
        d.ParentCode AS parent_code,
        -- Exploded attributes
        d.Corp_Region_Site AS corp_region_site,
        d.Business_unit AS business_unit,
        d.SVP_Subgroup AS svp_subgroup,
        d.DPT_FUSION_ID AS dpt_fusion_id,
        -- Hierarchy
        d.RollupLevels AS rollup_levels,
        d.current_level
    FROM deduped d
    LEFT JOIN deduped p
        ON d.ParentId = p.Department_Leaf_Line_ID
)

SELECT * FROM with_parent_key
