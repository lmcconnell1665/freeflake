{{ config(location=external_location('gold', 'finance/category_groups')) }}

-- Category group dimension: id and name only, for joining / labeling in BI.

select
    category_group_id     as category_group_id,
    category_group_name   as category_group_name
from {{ ref('silver_ynab__category_groups') }}
