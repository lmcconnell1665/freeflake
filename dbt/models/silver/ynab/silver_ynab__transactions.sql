{{ config(location=external_location('silver', 'ynab/transactions')) }}

with source as (
    select * from {{ source('ynab', 'transactions') }}
),

latest as (
    select *
    from source
    qualify row_number() over (partition by _budget_id, id order by _ingested_at desc) = 1
),

-- The budget-detail transactions array carries FK ids but not the names; join them.
accounts as (
    select budget_id, account_id, account_name from {{ ref('silver_ynab__accounts') }}
),
payees as (
    select budget_id, payee_id, payee_name from {{ ref('silver_ynab__payees') }}
),
categories as (
    select budget_id, category_id, category_name from {{ ref('silver_ynab__categories') }}
)

select
    t.id                                                    as transaction_id,
    t._budget_id                                            as budget_id,
    try_cast(t.date as date)                                as transaction_date,
    -- YNAB stores money as integer milliunits; divide by 1000 for currency units.
    t.amount / 1000.0                                       as amount,
    try_cast(t.memo as varchar)                             as memo,
    -- "cleared" / "uncleared" / "reconciled"
    t.cleared                                               as cleared_status,
    t.approved                                              as is_approved,
    t.flag_color                                            as flag_color,
    -- flag_name (custom flag label) is not returned by the budget-detail endpoint.
    cast(null as varchar)                                   as flag_name,
    t.account_id                                            as account_id,
    a.account_name                                          as account_name,
    t.payee_id                                              as payee_id,
    p.payee_name                                            as payee_name,
    t.category_id                                           as category_id,
    c.category_name                                         as category_name,
    -- Transfer / match ids are string ids in YNAB; cast through varchar since
    -- all-null ingest batches land them as integer.
    try_cast(cast(t.transfer_account_id as varchar) as varchar)     as transfer_account_id,
    try_cast(cast(t.transfer_transaction_id as varchar) as varchar) as transfer_transaction_id,
    try_cast(cast(t.matched_transaction_id as varchar) as varchar)  as matched_transaction_id,
    t.import_id                                             as import_id,
    t.import_payee_name                                     as import_payee_name,
    t.import_payee_name_original                            as import_payee_name_original,
    t.deleted                                               as is_deleted,
    strptime(t._ingested_at, '%Y%m%dT%H%M%SZ')              as ingested_at
from latest t
left join accounts a   on t._budget_id = a.budget_id and t.account_id  = a.account_id
left join payees p     on t._budget_id = p.budget_id and t.payee_id    = p.payee_id
left join categories c on t._budget_id = c.budget_id and t.category_id = c.category_id
