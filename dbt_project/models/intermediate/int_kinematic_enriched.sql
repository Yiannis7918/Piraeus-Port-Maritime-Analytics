-- models/intermediate/int_kinematic_enriched.sql

{{ config(materialized='table') }}

with static_filtered as (
    select 
        vessel_id,
        country,
        shiptype_code
    from {{ ref('stg_ais_static') }}
    where shiptype_code between 20 and 99 and shiptype_code not in (38, 39)
),

kinematic as (
    select * from {{ ref('stg_ais_kinematic') }}
)

select
    k.vessel_id,
    k.event_timestamp,
    k.event_date,
    k.lon,
    k.lat,
    k.speed,
    k.source_year,
    s.country,
    s.shiptype_code
from kinematic k
inner join static_filtered s
    on k.vessel_id = s.vessel_id