{{
  config(
    materialized='table',
    schema='db_gold',
    comment='Gold fact table for Planful financial data'
  )
}}

WITH source AS (
    SELECT
        gl.Scenario,
        gl._tenant_name,
        gl.FiscalYear,
        gl.FiscalMonth,
        gl.MonthName,
        gl.Account_Leaf_ID,
        gl.Company_Leaf_ID,
        gl.Intercompany_Leaf_ID,
        gl.Location_Leaf_ID,
        gl.Revenue_Type_Leaf_ID,
        gl.Department_Leaf_ID,
        gl.MSA_Leaf_ID,
        gl.GAN_Leaf_ID,
        gl.Reporting,
        gl.MtdAmount,
        gl.YtdAmount,
        gl.QtdAmount
    FROM {{ ref('planful_gl_data') }} gl
),

-- Look up surrogate keys from Silver dimension tables
with_dimension_keys AS (
    SELECT
        -- Scenario key (derived)
        SHA2(CONCAT_WS('|', 
            COALESCE(s.Scenario, ''),
            COALESCE(s._tenant_name, '')
        ), 256) AS scenario_key,
        
        -- Date key - join to existing Gold date dimension
        s.FiscalYear AS fiscal_year,
        s.FiscalMonth AS fiscal_month,
        s.MonthName AS month_name,
        
        -- Dimension surrogate keys (looked up from Silver)
        acct._account_key AS account_key,
        comp._company_key AS company_key,
        ic._company_key AS intercompany_key,
        loc._location_key AS location_key,
        rev._revtype_key AS revtype_key,
        dept._department_key AS department_key,
        msa._msa_key AS msa_key,
        gan._gan_key AS gan_key,
        
        -- Degenerate dimensions
        s.Reporting AS reporting,
        
        -- Measures
        s.MtdAmount AS mtd_amount,
        s.YtdAmount AS ytd_amount,
        s.QtdAmount AS qtd_amount
        
    FROM source s
    
    -- Account lookup
    LEFT JOIN {{ ref('planful_account') }} acct
        ON s.Account_Leaf_ID = acct.Account_Leaf_ID
        AND s._tenant_name = acct._tenant_name
        AND acct._is_deleted = false
    
    -- Company lookup
    LEFT JOIN {{ ref('planful_company') }} comp
        ON s.Company_Leaf_ID = comp.Company_ID
        AND s._tenant_name = comp._tenant_name
        AND comp._is_deleted = false
    
    -- Intercompany lookup (also from company dimension)
    LEFT JOIN {{ ref('planful_company') }} ic
        ON s.Intercompany_Leaf_ID = ic.Company_ID
        AND s._tenant_name = ic._tenant_name
        AND ic._is_deleted = false
    
    -- Location lookup
    LEFT JOIN {{ ref('planful_location') }} loc
        ON s.Location_Leaf_ID = loc.Location_Leaf_ID
        AND s._tenant_name = loc._tenant_name
        AND loc._is_deleted = false
    
    -- Revenue Type lookup
    LEFT JOIN {{ ref('planful_revtype') }} rev
        ON s.Revenue_Type_Leaf_ID = rev.Revenue_Type_Leaf_ID
        AND s._tenant_name = rev._tenant_name
        AND rev._is_deleted = false
    
    -- Department lookup
    LEFT JOIN {{ ref('planful_department') }} dept
        ON s.Department_Leaf_ID = dept.Department_Leaf_ID
        AND s._tenant_name = dept._tenant_name
        AND dept._is_deleted = false
    
    -- MSA lookup
    LEFT JOIN {{ ref('planful_msa') }} msa
        ON s.MSA_Leaf_ID = msa.MSA_Leaf_ID
        AND s._tenant_name = msa._tenant_name
        AND msa._is_deleted = false
    
    -- GAN lookup
    LEFT JOIN {{ ref('planful_gan') }} gan
        ON s.GAN_Leaf_ID = gan.GAN_Leaf_ID
        AND s._tenant_name = gan._tenant_name
        AND gan._is_deleted = false
)

SELECT
    scenario_key,
    fiscal_year,
    fiscal_month,
    month_name,
    account_key,
    company_key,
    intercompany_key,
    location_key,
    revtype_key,
    department_key,
    msa_key,
    gan_key,
    reporting,
    mtd_amount,
    ytd_amount,
    qtd_amount
FROM with_dimension_keys
