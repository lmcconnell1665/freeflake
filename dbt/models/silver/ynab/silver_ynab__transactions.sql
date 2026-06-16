{{ config(location=external_location('silver', 'ynab/transactions')) }}

with source as (
    select * from {{ source('ynab', 'transactions') }}
),

-- Bronze accumulates one row per record per ingest run; keep the newest version of each transaction.
latest as (
    select *
    from source
    qualify row_number() over (partition by _budget_id, id order by _ingested_at desc) = 1
)

select
    id                                                      as transaction_id,
    _budget_id                                              as budget_id,
    try_cast(date as date)                                  as transaction_date,
    -- YNAB stores money as integer milliunits; divide by 1000 for currency units.
    amount / 1000.0                                         as amount,
    try_cast(memo as varchar)                               as memo,
    -- "cleared" / "uncleared" / "reconciled"
    cleared                                                 as cleared_status,
    approved                                                as is_approved,
    flag_color                                              as flag_color,
    flag_name                                               as flag_name,
    account_id                                              as account_id,
    account_name                                            as account_name,
    payee_id                                                as payee_id,
    payee_name                                              as payee_name,
    category_id                                             as category_id,
    category_name                                           as category_name,
    -- Transfer / match ids are string ids in YNAB; cast through varchar since
    -- all-null ingest batches land them as integer.
    try_cast(cast(transfer_account_id as varchar) as varchar)     as transfer_account_id,
    try_cast(cast(transfer_transaction_id as varchar) as varchar) as transfer_transaction_id,
    try_cast(cast(matched_transaction_id as varchar) as varchar)  as matched_transaction_id,
    import_id                                               as import_id,
    import_payee_name                                       as import_payee_name,
    import_payee_name_original                              as import_payee_name_original,
    deleted                                                 as is_deleted,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                as ingested_at
from latest
