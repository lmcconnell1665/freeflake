{{ config(location=external_location('gold', 'finance/plans')) }}

-- Plan dimension: the YNAB "budgets" entity, exposed as plans to avoid clashing
-- with the budget-amount mart (gold_finance__budget). id and name, plus the
-- active month range.

select
    budget_id       as plan_id,
    budget_name     as plan_name,
    first_month     as first_month,
    last_month      as last_month
from {{ ref('silver_ynab__budgets') }}
