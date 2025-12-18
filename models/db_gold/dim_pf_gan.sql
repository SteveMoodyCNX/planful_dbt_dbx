{{
  config(
    materialized='table',
    schema='db_gold',
    comment='Gold dimension table for Planful GAN (Global Account Number)'
  )
}}

WITH source AS (
    SELECT
        s._gan_key,
        s.GAN_Leaf_ID,
        s.GAN,
        s.MemberType,
        s.ActiveStatus,
        s.ParentCode,
        s.ParentId,
        s.GAN_Leaf_Line_ID,
        -- Hierarchy
        s.RollupLevels,
        s.current_level,
        s._tenant_name
    FROM {{ ref('planful_gan') }} s
    WHERE s._is_deleted = false
),

-- Deduplicate: prefer Consolidation tenant, fallback to Transaction
deduped AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _gan_key 
        ORDER BY CASE WHEN _tenant_name = 'Consolidation' THEN 1 ELSE 2 END
    ) = 1
),

-- Self-join to get parent surrogate key
with_parent_key AS (
    SELECT
        d._gan_key AS gan_key,
        p._gan_key AS parent_gan_key,
        d.GAN_Leaf_ID AS gan_id,
        d.GAN AS gan_name,
        d.MemberType AS member_type,
        d.ActiveStatus AS active_status,
        d.ParentCode AS parent_code,
        -- Hierarchy
        d.RollupLevels AS rollup_levels,
        d.current_level
    FROM deduped d
    LEFT JOIN deduped p
        ON d.ParentId = p.GAN_Leaf_Line_ID
)

SELECT * FROM with_parent_key
