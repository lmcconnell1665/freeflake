{{ config(location=external_location('gold', 'finance/savings_balance')) }}

-- Savings balance mart: month-over-month assigned balance for savings buckets.
-- Shows amounts actually assigned (the YNAB category balance) each month to the
-- "Savings" category across all budgets, plus every category in the "Joint
-- Accounts" budget (excluding "Inflow: Ready to Assign").
--
-- amount_delta is the change in assigned balance versus the prior month for the
-- same budget + category. Amounts are already converted from milliunits to
-- currency units in silver.

with category_months as (
    select * from {{ ref('silver_ynab__category_months') }}
),

budgets as (
    select * from {{ ref('silver_ynab__budgets') }}
)

select
    cm.category_name        as category_name,
    b.budget_name           as account_name,
    cm.month                as budget_month,
    cm.balance              as amount,
    cm.balance - coalesce(
        lag(cm.balance) over (
            partition by b.budget_name, cm.category_name
            order by cm.month
        ),
        0
    )                       as amount_delta
from category_months cm
left join budgets b
    on cm.budget_id = b.budget_id
where cm.category_name = 'Savings'
   or (
       b.budget_name = 'Joint Accounts'
       and cm.category_name <> 'Inflow: Ready to Assign'
   )
