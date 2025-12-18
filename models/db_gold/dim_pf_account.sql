{{
  config(
    materialized='table',
    schema='db_gold',
    comment='Gold dimension table for Planful accounts'
  )
}}

WITH source AS (
    SELECT
        s._account_key,
        s.Account_Leaf_ID,
        s.Account,
        s.MemberType,
        s.ActiveStatus,
        s.AccountType,
        s.AccountGroup,
        s.NormalDataInput,
        s.CreditDebit,
        s.Variance,
        s.TrialBalanceAccount,
        s.ReportCategoryName,
        s.CurrencyTypeCode,
        s.CurrencyTypeName,
        s.ParentCode,
        s.ParentMemberCode,
        s.ParentMemberName,
        s.ParentMemberLabel,
        s.ParentMemberId,
        s.Account_Leaf_Line_ID,
        -- Exploded attributes
        s.ACCOUNT_GROUPING,
        s.SYNNEX_GROUPING,
        -- Hierarchy
        s.RollupLevels,
        s.current_level,
        s._tenant_name
    FROM {{ ref('planful_account') }} s
    WHERE s._is_deleted = false
),

-- Deduplicate: prefer Consolidation tenant, fallback to Transaction
deduped AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _account_key 
        ORDER BY CASE WHEN _tenant_name = 'Consolidation' THEN 1 ELSE 2 END
    ) = 1
),

-- Self-join to get parent surrogate key
with_parent_key AS (
    SELECT
        d._account_key AS account_key,
        p._account_key AS parent_account_key,
        d.Account_Leaf_ID AS account_id,
        d.Account AS account_name,
        d.MemberType AS member_type,
        d.ActiveStatus AS active_status,
        d.AccountType AS account_type,
        d.AccountGroup AS account_group,
        d.NormalDataInput AS normal_data_input,
        d.CreditDebit AS credit_debit,
        d.Variance AS variance,
        d.TrialBalanceAccount AS trial_balance_account,
        d.ReportCategoryName AS report_category_name,
        d.CurrencyTypeCode AS currency_type_code,
        d.CurrencyTypeName AS currency_type_name,
        d.ParentCode AS parent_code,
        d.ParentMemberCode AS parent_member_code,
        d.ParentMemberName AS parent_member_name,
        d.ParentMemberLabel AS parent_member_label,
        -- Exploded attributes
        d.ACCOUNT_GROUPING AS account_grouping,
        d.SYNNEX_GROUPING AS synnex_grouping,
        -- Hierarchy
        d.RollupLevels AS rollup_levels,
        d.current_level
    FROM deduped d
    LEFT JOIN deduped p
        ON d.ParentMemberId = p.Account_Leaf_Line_ID
)

SELECT * FROM with_parent_key
