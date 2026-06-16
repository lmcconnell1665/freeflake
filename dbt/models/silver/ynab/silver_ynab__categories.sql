{{ config(location=external_location('silver', 'ynab/categories')) }}

with source as (
    select * from {{ source('ynab', 'categories') }}
),

-- The categories endpoint nests the real categories inside each group row as a
-- JSON array. Dedupe groups first, then explode one row per category.
latest as (
    select *
    from source
    qualify row_number() over (partition by _budget_id, id order by _ingested_at desc) = 1
),

exploded as (
    select
        _budget_id,
        _ingested_at,
        unnest(json_extract(categories, '$[*]')) as category
    from latest
    where categories is not null
)

select
    json_extract_string(category, '$.id')                  as category_id,
    _budget_id                                             as budget_id,
    json_extract_string(category, '$.category_group_id')   as category_group_id,
    json_extract_string(category, '$.category_group_name') as category_group_name,
    json_extract_string(category, '$.name')                as category_name,
    try_cast(json_extract_string(category, '$.hidden') as boolean)   as is_hidden,
    try_cast(json_extract_string(category, '$.internal') as boolean) as is_internal,
    json_extract_string(category, '$.note')                as note,
    -- YNAB stores money as integer milliunits; divide by 1000 for currency units.
    try_cast(json_extract(category, '$.budgeted') as bigint) / 1000.0 as budgeted,
    try_cast(json_extract(category, '$.activity') as bigint) / 1000.0 as activity,
    try_cast(json_extract(category, '$.balance')  as bigint) / 1000.0 as balance,
    json_extract_string(category, '$.goal_type')           as goal_type,
    try_cast(json_extract(category, '$.goal_target') as bigint) / 1000.0 as goal_target,
    try_cast(json_extract_string(category, '$.goal_percentage_complete') as integer) as goal_percentage_complete,
    try_cast(json_extract_string(category, '$.deleted') as boolean)  as is_deleted,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')               as ingested_at
from exploded
