-- Staging model for AIS vessel static data.
-- Casts shiptype from FLOAT to INT (it's a code, not a continuous measure),
-- drops rows with no vessel_id (untrackable vessels).

with source as (
    select * from {{ source('piraeus_raw', 'unipi_ais_static') }}
),

cleaned as (
    select
        vessel_id,
        country,
        cast(shiptype as int64) as shiptype_code
    from source
    where vessel_id is not null
)

select * from cleaned