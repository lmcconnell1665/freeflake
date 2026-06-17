{{ config(location=external_location('silver', 'homeassistant/states')) }}

with source as (
    select * from {{ source('homeassistant', 'states') }}
),

-- Bronze overlaps: watermark ingests re-land the boundary record each run, so
-- the same (entity_id, last_updated) appears in multiple files. Keep the newest.
latest as (
    select *
    from source
    qualify row_number() over (partition by entity_id, last_updated order by _ingested_at desc) = 1
)

select
    entity_id                                       as entity_id,
    -- "sensor", "climate", ... — the part before the first dot.
    split_part(entity_id, '.', 1)                   as domain,
    json_extract_string(attributes, '$.friendly_name') as friendly_name,
    state                                           as state,
    -- States are reported as strings; surface a numeric view when it parses.
    try_cast(state as double)                        as state_numeric,
    try_cast(attributes as json)                     as attributes,
    try_cast(last_changed as timestamptz)            as last_changed_at,
    try_cast(last_updated as timestamptz)            as last_updated_at,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')         as ingested_at
from latest
