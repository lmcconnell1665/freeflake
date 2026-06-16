{{ config(location=external_location('silver', 'quickbooks/invoices')) }}

with source as (
    select * from {{ source('quickbooks', 'invoices') }}
),

-- Bronze accumulates one row per record per ingest run; keep the newest version of each invoice.
latest as (
    select *
    from source
    qualify row_number() over (partition by Id order by _ingested_at desc) = 1
)

select
    Id                                                                              as invoice_id,
    DocNumber                                                                       as doc_number,
    json_extract_string(cast(CustomerRef as varchar), '$.value')                    as customer_id,
    json_extract_string(cast(CustomerRef as varchar), '$.name')                     as customer_name,
    try_cast(TxnDate as date)                                                       as txn_date,
    try_cast(DueDate as date)                                                       as due_date,
    try_cast(TotalAmt as double)                                                    as total_amount,
    try_cast(Balance as double)                                                     as balance,
    EmailStatus                                                                     as email_status,
    PrintStatus                                                                     as print_status,
    json_extract_string(cast(BillEmail as varchar), '$.Address')                    as bill_email,
    PrivateNote                                                                     as private_note,
    try_cast(json_extract_string(cast(MetaData as varchar), '$.CreateTime') as timestamp)      as created_at,
    try_cast(json_extract_string(cast(MetaData as varchar), '$.LastUpdatedTime') as timestamp) as updated_at,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                                        as ingested_at
from latest
