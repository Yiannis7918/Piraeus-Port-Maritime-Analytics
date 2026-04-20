-- Staging model for AIS kinematic (position) data.
-- Mechanical cleaning only: type conversion, column pruning, true data-quality filtering.
-- No geographic scoping — that's intermediate's job.

with source as (
    select * from {{ source('piraeus_raw', 'ais_kinematic') }}
),

cleaned as (
    select
        vessel_id,
        timestamp_millis(timestamp_ms) as event_timestamp,
        date(timestamp_millis(timestamp_ms)) as event_date,
        lon,
        lat,
        speed,
        source_year
    from source
    where
        vessel_id is not null
        and timestamp_ms is not null
        and lon is not null
        and lat is not null
        -- Valid coordinate ranges (planet Earth, not just Saronic Gulf)
        and lat between -90 and 90
        and lon between -180 and 180
        -- Drop the classic (0, 0) GPS-failure sentinel
        and not (lat = 0 and lon = 0)
        -- Speed sanity check: ships don't exceed ~50 knots
        and (speed is null or speed between 0 and 50)
)

select * from cleaned