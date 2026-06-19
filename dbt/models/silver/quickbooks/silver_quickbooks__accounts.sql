{{ config(location=external_location('silver', 'quickbooks/accounts')) }}

with source as (
    select * from {{ source('quickbooks', 'accounts') }}
),

latest as (
    select *
    from source
    qualify row_number() over (partition by Id order by _ingested_at desc) = 1
)

select
    Id                                                                              as account_id,
    Name                                                                            as account_name,
    FullyQualifiedName                                                              as fully_qualified_name,
    AccountType                                                                     as account_type,
    AccountSubType                                                                  as account_sub_type,
    Classification                                                                  as classification,
    AcctNum                                                                         as account_number,
    Description                                                                     as description,
    try_cast(Active as boolean)                                                     as is_active,
    try_cast(CurrentBalance as double)                                              as current_balance,
    try_cast(CurrentBalanceWithSubAccounts as double)                               as current_balance_with_sub_accounts,
    try_cast(json_extract_string(cast(MetaData as varchar), '$.CreateTime') as timestamp)      as created_at,
    try_cast(json_extract_string(cast(MetaData as varchar), '$.LastUpdatedTime') as timestamp) as updated_at,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                                        as ingested_at
from latest
