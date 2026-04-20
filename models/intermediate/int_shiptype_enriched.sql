-- Intermediate model for ship type lookup.
-- Takes the cleaned stg_ais_codes and adds a shiptype_category
-- using keyword matching on the description.
-- Category order matters: first match wins.

with codes as (
    select * from {{ ref('stg_ais_codes') }}
    where shiptype_code >= 20 and shiptype_code not in (38,39)
),

categorized as (
    select
        shiptype_code,
        shiptype,
        case
            when lower(shiptype) like '%tanker%'    then 'Tanker'
            when lower(shiptype) like '%passenger%' then 'Passenger'
            when lower(shiptype) like '%cargo%'     then 'Cargo'
            when lower(shiptype) like '%other type%' then 'Other Type'
            when lower(shiptype) like '%high speed craft (hsc)%' then 'HSC (High Speed Craft)'
            when lower(shiptype) like '%wing in ground (wig)%' then 'WIG (Wing in Ground)'
            else shiptype  -- fallback: keep original name
        end as shiptype_category
    from codes
)

select * from categorized