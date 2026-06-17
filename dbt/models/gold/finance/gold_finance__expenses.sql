{{ config(location=external_location('gold', 'finance/expenses')) }}

-- Expenses mart: one row per outflow (spending) transaction, with budget,
-- category group, category and payee names resolved. This is the mirror of the
-- income mart -- we exclude YNAB's "Inflow" categories so only real spending
-- remains.
--
-- Amounts are flipped to positive: YNAB stores outflows as negatives (already
-- converted from milliunits in silver), so we multiply by -1 to express spend
-- as a positive number.
--
-- Hidden categories and blue-flagged transactions are excluded by convention
-- (blue flags mark transfers / reimbursements that shouldn't count as spend).

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
    c.category_group_name   as category_group,
    c.category_name         as category,
    p.payee_name            as payee,
    t.transaction_date      as date,
    t.amount * -1           as amount,
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
where c.category_name not like '%Inflow%'
  and not coalesce(c.is_hidden, false)
  and coalesce(t.flag_color, '') <> 'blue'
  and t.transaction_date >= date '2025-01-01'
  and not coalesce(t.is_deleted, false)
