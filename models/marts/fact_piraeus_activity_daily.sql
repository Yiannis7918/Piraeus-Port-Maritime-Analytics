-- Daily aggregated fact table for Piraeus vessel activity.
-- Grain: one row per (event_date, shiptype_category, zone).
-- Source: int_positions_zoned (filtered to territorial waters, labeled at_port / in_approaches)
--
-- This is the primary query target for the Looker Studio dashboard.
-- Pre-aggregated from ~200M ping rows to ~17K daily rows for query performance.

{{ config(
    materialized='table',
    partition_by={
      'field': 'event_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['shiptype_category', 'zone']
) }}

with positions as (
    select * from {{ ref('int_positions_zoned') }}
),

shiptype as (
    select 
        shiptype_code,
        shiptype_category
    from {{ ref('int_shiptype_enriched') }}
),

enriched as (
    select
        p.event_date,
        p.vessel_id,
        p.country,
        p.zone,
        p.speed,
        s.shiptype_category
    from positions p
    left join shiptype s
        on p.shiptype_code = s.shiptype_code
),

aggregated as (
    select
        event_date,
        shiptype_category,
        zone,
        count(*) as ping_count,
        count(distinct vessel_id) as distinct_vessels,
        count(distinct country) as distinct_countries,
        avg(speed) as avg_speed
    from enriched
    group by event_date, shiptype_category, zone
)

select * from aggregated