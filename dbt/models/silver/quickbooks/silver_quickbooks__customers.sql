{{ config(location=external_location('silver', 'quickbooks/customers')) }}

with source as (
    select * from {{ source('quickbooks', 'customers') }}
),

latest as (
    select *
    from source
    qualify row_number() over (partition by Id order by _ingested_at desc) = 1
)

select
    Id                                                                              as customer_id,
    DisplayName                                                                     as display_name,
    CompanyName                                                                     as company_name,
    GivenName                                                                       as given_name,
    FamilyName                                                                      as family_name,
    try_cast(Active as boolean)                                                     as is_active,
    try_cast(Balance as double)                                                     as balance,
    json_extract_string(cast(PrimaryEmailAddr as varchar), '$.Address')             as email,
    json_extract_string(cast(BillAddr as varchar), '$.City')                        as billing_city,
    json_extract_string(cast(BillAddr as varchar), '$.CountrySubDivisionCode')      as billing_state,
    json_extract_string(cast(BillAddr as varchar), '$.PostalCode')                  as billing_postal_code,
    try_cast(json_extract_string(cast(MetaData as varchar), '$.CreateTime') as timestamp)      as created_at,
    try_cast(json_extract_string(cast(MetaData as varchar), '$.LastUpdatedTime') as timestamp) as updated_at,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                                        as ingested_at
from latest
