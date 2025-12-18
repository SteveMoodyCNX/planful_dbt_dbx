{{
  config(
    materialized='table',
    schema='db_gold',
    comment='Gold dimension table for Planful MSA (Master Service Agreement)'
  )
}}

WITH source AS (
    SELECT
        s._msa_key,
        s.MSA_Leaf_ID,
        s.MSA,
        s.MemberType,
        s.ActiveStatus,
        s.ParentCode,
        s.ParentId,
        s.MSA_Leaf_Line_ID,
        -- Exploded attributes
        s.Industry,
        s.MDA,
        s.Bus_Unit_L3,
        s.MSA_FUSION_ID,
        s.MSA_Rev_Type,
        s.Client_Sub_Program_Main,
        -- Hierarchy
        s.RollupLevels,
        s.current_level,
        s._tenant_name
    FROM {{ ref('planful_msa') }} s
    WHERE s._is_deleted = false
),

-- Deduplicate: prefer Consolidation tenant, fallback to Transaction
deduped AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _msa_key 
        ORDER BY CASE WHEN _tenant_name = 'Consolidation' THEN 1 ELSE 2 END
    ) = 1
),

-- Self-join to get parent surrogate key
with_parent_key AS (
    SELECT
        d._msa_key AS msa_key,
        p._msa_key AS parent_msa_key,
        d.MSA_Leaf_ID AS msa_id,
        d.MSA AS msa_name,
        d.MemberType AS member_type,
        d.ActiveStatus AS active_status,
        d.ParentCode AS parent_code,
        -- Exploded attributes
        d.Industry AS industry,
        d.MDA AS mda,
        d.Bus_Unit_L3 AS bus_unit_l3,
        d.MSA_FUSION_ID AS msa_fusion_id,
        d.MSA_Rev_Type AS msa_rev_type,
        d.Client_Sub_Program_Main AS client_sub_program_main,
        -- Hierarchy
        d.RollupLevels AS rollup_levels,
        d.current_level
    FROM deduped d
    LEFT JOIN deduped p
        ON d.ParentId = p.MSA_Leaf_Line_ID
)

SELECT * FROM with_parent_key
