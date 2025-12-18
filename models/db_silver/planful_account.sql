{{
  config(
    materialized='incremental',
    unique_key=['_account_key','_tenant_name'],
	incremental_strategy='merge',
	on_schema_change = 'sync_all_columns',
	options = {"mergeSchema": "true"},
    schema='db_silver',
    comment='Silver table for Planful account hierarchy with flattened rollup levels'
  )
}}



WITH source_data_raw AS (

SELECT 
  MemberId AS `Account_Leaf_Line_ID`,
  Code AS `Account_Leaf_ID`,
  Name AS `Account`,
  SHA2(CONCAT_WS('|', 
      COALESCE(Code, ''),
      COALESCE(Name, '')
  ), 256) AS `_account_key`,
  RollupOperator,
  MemberType,
  ActiveStatus,
  ParentMemberId,
  AccountType,
  AccountGroup,
  NormalDataInput,
  CreditDebit,
  Variance,
  TrialBalanceAccount,
  ReportCategoryName,
  CurrencyTypeCode,
  CurrencyTypeName,
  ParentCode,
  ParentMemberCode,
  ParentMemberName,
  ParentMemberLabel,
  Attributes,
  {{ generate_attribute_columns(['ACCOUNT_GROUPING','SYNNEX_GROUPING']) }},
  RollupLevels,
  COALESCE(array_max(transform(RollupLevels, x -> CAST(x.Level AS INT))), 0) + 1 AS current_level,

  {{ generate_rollup_columns('Account', 11, include_hash=False) }},

    _source_loaded_at,
  _source_file,
  _tenant_name,
  Sort
FROM {{ source('db_bronze', 'planful_account') }}
),

source_data AS (
  SELECT * EXCEPT(Sort)
  FROM source_data_raw
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _account_key, _tenant_name ORDER BY Sort ASC) = 1
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
		COALESCE(CAST(Account_Leaf_Line_ID AS STRING), ''),
		COALESCE(CAST(RollupOperator AS STRING), ''),
		COALESCE(CAST(MemberType AS STRING), ''),
		COALESCE(CAST(ActiveStatus AS STRING), ''),
		COALESCE(CAST(ParentMemberId AS STRING), ''),
		COALESCE(CAST(AccountType AS STRING), ''),
		COALESCE(CAST(AccountGroup AS STRING), ''),
		COALESCE(CAST(NormalDataInput AS STRING), ''),
		COALESCE(CAST(CreditDebit AS STRING), ''),
		COALESCE(CAST(Variance AS STRING), ''),
		COALESCE(CAST(TrialBalanceAccount AS STRING), ''),
		COALESCE(CAST(ReportCategoryName AS STRING), ''),
		COALESCE(CAST(CurrencyTypeCode AS STRING), ''),
		COALESCE(CAST(CurrencyTypeName AS STRING), ''),
		COALESCE(CAST(ParentCode AS STRING), ''),
		COALESCE(CAST(ParentMemberCode AS STRING), ''),
		COALESCE(CAST(ParentMemberName AS STRING), ''),
		COALESCE(CAST(ParentMemberLabel AS STRING), ''),
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
	t.Account_Leaf_Line_ID,
	t.Account_Leaf_ID,
	t.Account,
	t._account_key,
	t.RollupOperator,
	t.MemberType,
	t.ActiveStatus,
	t.ParentMemberId,
	t.AccountType,
	t.AccountGroup,
	t.NormalDataInput,
	t.CreditDebit,
	t.Variance,
	t.TrialBalanceAccount,
	t.ReportCategoryName,
	t.CurrencyTypeCode,
	t.CurrencyTypeName,
	t.ParentCode,
	t.ParentMemberCode,
	t.ParentMemberName,
	t.ParentMemberLabel,
	t.Attributes,
	{{ generate_attribute_columns(['ACCOUNT_GROUPING','SYNNEX_GROUPING'],  table_alias='t') }},
	t.RollupLevels,
	t.current_level,
	{{ generate_rollup_columns('Account', 11, table_alias='t', include_hash=False) }},
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
	ON t._account_key = s._account_key
	AND t._tenant_name = s._tenant_name
  WHERE s._account_key IS NULL
    AND t._is_deleted = FALSE
),
    
-- Determine if records are updates or inserts
source_with_change_type AS (
  SELECT 
    s.*,
    CASE 
      WHEN t._account_key IS NULL THEN 'INSERT'
      WHEN s._record_hash != t._record_hash THEN 'UPDATE'
      ELSE 'NO_CHANGE'
    END AS actual_change_type
  FROM source_with_metadata s
  LEFT JOIN {{ this }} t 
    ON s._account_key = t._account_key 
	AND s._tenant_name = t._tenant_name
    AND t._is_deleted = FALSE
),

final_source AS (
  SELECT 
	Account_Leaf_Line_ID,
	Account_Leaf_ID,
	Account,
	_account_key,
	RollupOperator,
	MemberType,
	ActiveStatus,
	ParentMemberId,
	AccountType,
	AccountGroup,
	NormalDataInput,
	CreditDebit,
	Variance,
	TrialBalanceAccount,
	ReportCategoryName,
	CurrencyTypeCode,
	CurrencyTypeName,
	ParentCode,
	ParentMemberCode,
	ParentMemberName,
	ParentMemberLabel,
	Attributes,
	{{ generate_attribute_columns(['ACCOUNT_GROUPING','SYNNEX_GROUPING']) }},
	RollupLevels,
	current_level,
	{{ generate_rollup_columns('Account', 11, include_hash=False) }},
	_source_loaded_at,
	_source_file,
	_tenant_name,
    _processed_at,
    _is_deleted,
    _deleted_at,
    actual_change_type AS _change_type,
    _record_hash
  FROM source_with_change_type
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