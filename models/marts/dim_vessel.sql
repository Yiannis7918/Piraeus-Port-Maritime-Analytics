-- Vessel dimension.
-- One row per vessel, with country and shiptype info.
-- Derived from the cleaned, filtered vessel set in int_kinematic_enriched.

with vessels as (
    -- Get distinct vessels from the enriched kinematic table.
    -- These are vessels that: (a) appear in the AIS data, (b) have a known shiptype,
    -- and (c) passed our shiptype filter (shiptype >= 20 and not in 38, 39).
    select distinct
        vessel_id,
        country,
        shiptype_code
    from {{ ref('int_kinematic_enriched') }}
),

joined as (
    select
        v.vessel_id,
        v.country,
        v.shiptype_code,
        s.shiptype,
        s.shiptype_category
    from vessels v
    left join {{ ref('int_shiptype_enriched') }} s
        on v.shiptype_code = s.shiptype_code
)

select * from joined