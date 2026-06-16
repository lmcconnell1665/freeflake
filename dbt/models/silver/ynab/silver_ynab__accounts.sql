{{ config(location=external_location('silver', 'ynab/accounts')) }}

with source as (
    select * from {{ source('ynab', 'accounts') }}
),

-- Bronze accumulates one row per record per ingest run; keep the newest version of each account.
-- Account ids are unique within a budget, so partition by budget + id.
latest as (
    select *
    from source
    qualify row_number() over (partition by _budget_id, id order by _ingested_at desc) = 1
)

select
    id                                                      as account_id,
    _budget_id                                              as budget_id,
    name                                                    as account_name,
    type                                                    as account_type,
    on_budget                                               as is_on_budget,
    closed                                                  as is_closed,
    note                                                    as note,
    -- YNAB stores money as integer milliunits; divide by 1000 for currency units.
    balance / 1000.0                                        as balance,
    cleared_balance / 1000.0                                as cleared_balance,
    uncleared_balance / 1000.0                              as uncleared_balance,
    transfer_payee_id                                       as transfer_payee_id,
    direct_import_linked                                    as is_direct_import_linked,
    direct_import_in_error                                  as has_direct_import_error,
    try_cast(cast(last_reconciled_at as varchar) as timestamptz) as last_reconciled_at,
    deleted                                                 as is_deleted,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                as ingested_at
from latest
