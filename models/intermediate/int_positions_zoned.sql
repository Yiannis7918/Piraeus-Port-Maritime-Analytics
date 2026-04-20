-- Intermediate model: zoned position pings.
-- Uses pre-simplified polygons from int_piraeus_geography.

{{ config(
    materialized='table',
    partition_by={
      'field': 'event_date',
      'data_type': 'date',
      'granularity': 'month'
    }
) }}

with positions as (
    select * from {{ ref('int_kinematic_enriched') }}
),

port_polygon as (
    select geography
    from {{ ref('int_piraeus_geography') }}
    where zone_type = 'at_port'
),

territorial_polygon as (
    select geography
    from {{ ref('int_piraeus_geography') }}
    where zone_type = 'territorial'
),

zoned as (
    select
        p.vessel_id,
        p.event_timestamp,
        p.event_date,
        p.lon,
        p.lat,
        p.speed,
        p.source_year,
        p.country,
        p.shiptype_code,
        case
            when st_contains(port.geography, st_geogpoint(p.lon, p.lat)) then 'at_port'
            else 'in_approaches'
        end as zone
    from positions p
    cross join port_polygon port
    cross join territorial_polygon territorial
    where st_contains(territorial.geography, st_geogpoint(p.lon, p.lat))
)

select * from zoned