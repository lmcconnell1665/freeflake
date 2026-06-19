{{ config(location=external_location('silver', 'ynab/budgets')) }}

with source as (
    select * from {{ source('ynab', 'budgets') }}
),

latest as (
    select *
    from source
    qualify row_number() over (partition by id order by _ingested_at desc) = 1
)

select
    id                                                      as budget_id,
    name                                                    as budget_name,
    try_cast(first_month as date)                           as first_month,
    try_cast(last_month as date)                            as last_month,
    try_cast(cast(last_modified_on as varchar) as timestamptz) as last_modified_on,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                as ingested_at
from latest
