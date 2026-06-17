{{ config(location=external_location('gold', 'finance/income')) }}

-- Income mart: one row per income (Inflow) transaction, with budget and payee
-- names resolved. YNAB records inflows against an "Inflow: Ready to Assign"
-- category, so we filter to category names containing "Inflow".
--
-- Blue-flagged transactions are excluded by convention (used to mark transfers /
-- reimbursements that shouldn't count as real income).

with transactions as (
    select * from {{ ref('silver_ynab__transactions') }}
),

categories as (
    select * from {{ ref('silver_ynab__categories') }}
),

budgets as (
    select * from {{ ref('silver_ynab__budgets') }}
),

payees as (
    select * from {{ ref('silver_ynab__payees') }}
)

select
    b.budget_name           as budget,
    p.payee_name            as payee,
    t.transaction_date      as date,
    -- silver already converts YNAB milliunits to currency units.
    t.amount                as amount,
    t.category_id           as category_id,
    t.payee_id              as payee_id
from transactions t
join categories c
    on  t.budget_id   = c.budget_id
    and t.category_id = c.category_id
join budgets b
    on c.budget_id = b.budget_id
join payees p
    on  t.budget_id = p.budget_id
    and t.payee_id  = p.payee_id
where c.category_name like '%Inflow%'
  and coalesce(t.flag_color, '') <> 'blue'
  and t.transaction_date >= date '2025-01-01'
  -- exclude deleted transactions; silver retains them for auditability.
  and not coalesce(t.is_deleted, false)
