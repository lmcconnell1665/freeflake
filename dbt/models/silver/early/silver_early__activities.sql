{{ config(location=external_location('silver', 'early/activities')) }}

with source as (
    select * from {{ source('early', 'activities') }}
),

latest as (
    select *
    from source
    qualify row_number() over (partition by id order by _ingested_at desc) = 1
)

select
    id                                          as activity_id,
    name                                        as activity_name,
    color                                       as color,
    folderId                                    as folder_id,
    -- "active" / "inactive" / "archived"
    status                                      as status,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')    as ingested_at
from latest
