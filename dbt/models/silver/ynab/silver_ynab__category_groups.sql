{{ config(location=external_location('silver', 'ynab/category_groups')) }}

with source as (
    select * from {{ source('ynab', 'category_groups') }}
),

-- Bronze accumulates one row per group per ingest run (delta loads only write
-- changed groups); keep the newest version of each group.
latest as (
    select *
    from source
    qualify row_number() over (partition by _budget_id, id order by _ingested_at desc) = 1
)

select
    id                                                      as category_group_id,
    _budget_id                                              as budget_id,
    name                                                    as category_group_name,
    hidden                                                  as is_hidden,
    -- Internal groups (e.g. "Internal Master Category") are YNAB system buckets.
    internal                                                as is_internal,
    deleted                                                 as is_deleted,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                as ingested_at
from latest
