{{
  config(
    materialized='table',
    schema='db_gold',
    comment='Gold dimension table for Planful companies'
  )
}}

WITH source AS (
    SELECT
        s._company_key,
        s.Company_ID,
        s.Company,
        s.MemberType,
        s.ActiveStatus,
        s.CurrencyCode,
        s.CurrencyName,
        s.ParentCode,
        s.ParentId,
        s.Company_Leaf_Line_ID,
        -- Exploded attributes
        s.Country,
        s.SITE,
        s.SITE_DESC,
        -- Hierarchy
        s.RollupLevels,
        s.current_level,
        s._tenant_name
    FROM {{ ref('planful_company') }} s
    WHERE s._is_deleted = false
),

-- Deduplicate: prefer Consolidation tenant, fallback to Transaction
deduped AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _company_key 
        ORDER BY CASE WHEN _tenant_name = 'Consolidation' THEN 1 ELSE 2 END
    ) = 1
),

-- Self-join to get parent surrogate key
with_parent_key AS (
    SELECT
        d._company_key AS company_key,
        p._company_key AS parent_company_key,
        d.Company_ID AS company_id,
        d.Company AS company_name,
        d.MemberType AS member_type,
        d.ActiveStatus AS active_status,
        d.CurrencyCode AS currency_code,
        d.CurrencyName AS currency_name,
        d.ParentCode AS parent_code,
        -- Exploded attributes
        d.Country AS country,
        d.SITE AS site,
        d.SITE_DESC AS site_desc,
        -- Hierarchy
        d.RollupLevels AS rollup_levels,
        d.current_level
    FROM deduped d
    LEFT JOIN deduped p
        ON d.ParentId = p.Company_Leaf_Line_ID
)

SELECT * FROM with_parent_key
