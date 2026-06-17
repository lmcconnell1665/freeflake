{{ config(location=external_location('silver', 'ynab/budget_targets')) }}

-- Hand-maintained 2026 budget plan, loaded from the finance_budget_2026 seed.
-- The seed is wide (one column per budget month); here we unpivot it into the
-- long grain one row per budget + category + month. DuckDB's UNPIVOT drops NULL
-- month cells, so months with no planned amount simply don't produce a row.

with source as (
    select * from {{ ref('finance_budget_2026') }}
),

unpivoted as (
    unpivot source
    on
        "2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01",
        "2026-05-01", "2026-06-01", "2026-07-01", "2026-08-01",
        "2026-09-01", "2026-10-01", "2026-11-01", "2026-12-01"
    into
        name  budget_month
        value budget_amount
)

select
    budget_id                       as budget_id,
    category_id                     as category_id,
    cast(budget_month as date)      as budget_month,
    cast(budget_amount as double)   as budget_amount
from unpivoted
