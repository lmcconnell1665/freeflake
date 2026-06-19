{{ config(location=external_location('silver', 'quickbooks/payments')) }}

with source as (
    select * from {{ source('quickbooks', 'payments') }}
),

latest as (
    select *
    from source
    qualify row_number() over (partition by Id order by _ingested_at desc) = 1
)

select
    Id                                                                              as payment_id,
    json_extract_string(cast(CustomerRef as varchar), '$.value')                    as customer_id,
    json_extract_string(cast(CustomerRef as varchar), '$.name')                     as customer_name,
    try_cast(TxnDate as date)                                                       as txn_date,
    try_cast(TotalAmt as double)                                                    as total_amount,
    try_cast(UnappliedAmt as double)                                                as unapplied_amount,
    json_extract_string(cast(PaymentMethodRef as varchar), '$.name')                as payment_method,
    json_extract_string(cast(DepositToAccountRef as varchar), '$.name')             as deposit_to_account,
    PrivateNote                                                                     as private_note,
    try_cast(json_extract_string(cast(MetaData as varchar), '$.CreateTime') as timestamp)      as created_at,
    try_cast(json_extract_string(cast(MetaData as varchar), '$.LastUpdatedTime') as timestamp) as updated_at,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                                        as ingested_at
from latest
