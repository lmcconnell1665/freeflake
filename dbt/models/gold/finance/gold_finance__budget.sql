{{ config(location=external_location('gold', 'finance/budget')) }}

-- Budget mart: planned budget amount per budget + category + month, from the
-- hand-maintained 2026 plan. Budget and category names are resolved from their
-- silver dimensions so this lines up with the income / expenses marts. Rows
-- whose category isn't in the YNAB category dimension are kept (left join) so a
-- mislabeled plan row is still visible rather than silently dropped.

with budget_targets as (
    select * from {{ ref('silver_ynab__budget_targets') }}
),

categories as (
    select * from {{ ref('silver_ynab__categories') }}
),

budgets as (
    select * from {{ ref('silver_ynab__budgets') }}
)

select
    b.budget_name           as budget,
    c.category_group_name   as category_group,
    c.category_name         as category,
    t.budget_month          as budget_month,
    t.budget_amount         as budget_amount,
    t.budget_id             as budget_id,
    t.category_id           as category_id
from budget_targets t
left join budgets b
    on t.budget_id = b.budget_id
left join categories c
    on  t.budget_id   = c.budget_id
    and t.category_id = c.category_id
