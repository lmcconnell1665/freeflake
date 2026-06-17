{{ config(location=external_location('gold', 'finance/categories')) }}

-- Category dimension: id and name only, for joining / labeling in BI.

select
    category_id     as category_id,
    category_name   as category_name
from {{ ref('silver_ynab__categories') }}
