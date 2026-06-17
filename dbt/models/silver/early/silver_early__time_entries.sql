{{ config(location=external_location('silver', 'early/time_entries')) }}

with source as (
    select * from {{ source('early', 'time_entries') }}
),

-- Full history is re-dumped each run (the API has no modified-since cursor), so
-- an edited entry reappears under the same id — keep the newest version.
latest as (
    select *
    from source
    qualify row_number() over (partition by id order by _ingested_at desc) = 1
)

select
    id                                                              as time_entry_id,
    -- activity is embedded per entry rather than referenced by id alone.
    json_extract_string(activity, '$.id')                          as activity_id,
    json_extract_string(activity, '$.name')                        as activity_name,
    try_cast(json_extract_string(duration, '$.startedAt') as timestamp) as started_at,
    try_cast(json_extract_string(duration, '$.stoppedAt') as timestamp) as stopped_at,
    date_diff(
        'minute',
        try_cast(json_extract_string(duration, '$.startedAt') as timestamp),
        try_cast(json_extract_string(duration, '$.stoppedAt') as timestamp)
    )                                                              as duration_minutes,
    json_extract_string(note, '$.text')                            as note_text,
    json_extract(note, '$.tags')                                   as note_tags,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                       as ingested_at
from latest
