{{ config(location=external_location('gold', 'finance/net_worth')) }}

-- Net worth mart: one row per account per month, carrying the running balance
-- of every open account from its first activity through the current month.
--
-- Months with no transactions still get a row (via a dense month spine) so the
-- running balance carries forward across gaps -- this gives a smooth, gap-free
-- series suitable for charting net worth over time.

with transactions as (
    select * from {{ ref('silver_ynab__transactions') }}
),

accounts as (
    select * from {{ ref('silver_ynab__accounts') }}
),

budgets as (
    select * from {{ ref('silver_ynab__budgets') }}
),

-- Net transaction activity per open account per month.
monthly_balances as (
    select
        a.account_name                            as account,
        a.account_type                            as account_type,
        a.note                                    as account_note,
        b.budget_name                             as budget,
        date_trunc('month', t.transaction_date)   as year_month,
        sum(t.amount)                             as balance
    from accounts a
    join transactions t
        on  a.budget_id  = t.budget_id
        and a.account_id = t.account_id
    join budgets b
        on a.budget_id = b.budget_id
    where not coalesce(a.is_closed, false)
      and not coalesce(t.is_deleted, false)
    group by 1, 2, 3, 4, 5
),

-- The first month each account saw activity; the spine starts here per account.
account_spine as (
    select
        account,
        account_type,
        account_note,
        budget,
        min(year_month) as first_month
    from monthly_balances
    group by 1, 2, 3, 4
),

-- A dense list of months from the earliest activity through the current month.
month_spine as (
    select cast(range as date) as year_month
    from range(
        (select min(year_month) from monthly_balances),
        date_trunc('month', current_date) + interval '1 month',
        interval '1 month'
    )
),

-- One row per account for every month from its first activity onward.
spine as (
    select
        a.account,
        a.account_type,
        a.account_note,
        a.budget,
        m.year_month
    from account_spine a
    cross join month_spine m
    where m.year_month >= a.first_month
)

select
    s.account,
    s.account_type,
    s.account_note,
    s.budget,
    s.year_month,
    sum(coalesce(mb.balance, 0)) over (
        partition by s.account, s.budget
        order by s.year_month
    ) as balance
from spine s
left join monthly_balances mb
    on  s.account    = mb.account
    and s.budget     = mb.budget
    and s.year_month = mb.year_month
