{{ config(location=external_location('gold', 'finance/payees')) }}

-- Payee dimension: id and name only, for joining / labeling in BI.

select
    payee_id     as payee_id,
    payee_name   as payee_name
from {{ ref('silver_ynab__payees') }}
