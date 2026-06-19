{{ config(location=external_location('silver', 'ynab/payees')) }}

with source as (
    select * from {{ source('ynab', 'payees') }}
),

latest as (
    select *
    from source
    qualify row_number() over (partition by _budget_id, id order by _ingested_at desc) = 1
)

select
    id                                                      as payee_id,
    _budget_id                                              as budget_id,
    name                                                    as payee_name,
    -- Populated when the payee represents a transfer to another account.
    transfer_account_id                                     as transfer_account_id,
    deleted                                                 as is_deleted,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                as ingested_at
from latest
