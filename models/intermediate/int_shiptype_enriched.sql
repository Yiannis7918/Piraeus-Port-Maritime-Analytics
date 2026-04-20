-- Intermediate model: shiptype lookup with category assignment.
-- Uses keyword matching on shiptype_name to group codes into 8 analytical categories.
-- Pattern order matters: first match wins.

with codes as (
    select * from {{ ref('stg_ais_codes') }}
    where shiptype_code between 20 and 99
      and shiptype_code not in (38, 39)
),

categorized as (
    select
        shiptype_code,
        shiptype,
        case
            -- Core commercial traffic
            when lower(shiptype) like '%tanker%'       then 'Tanker'
            when lower(shiptype) like '%cargo%'        then 'Cargo'
            when lower(shiptype) like '%passenger%'    then 'Passenger'
            
            -- Fast transport (HSC + WIG grouped)
            when lower(shiptype) like '%high speed craft%' then 'Fast Transport'
            when lower(shiptype) like '%wing in ground%'   then 'Fast Transport'
            
            -- Recreational (pleasure + sailing grouped)
            when lower(shiptype) like '%pleasure%'     then 'Recreational'
            when lower(shiptype) like '%sailing%'      then 'Recreational'
            
            -- Fishing
            when lower(shiptype) like '%fishing%'      then 'Fishing'
            
            -- Port services (tugs, pilot, rescue, enforcement, etc.)
            when lower(shiptype) like '%tug%'              then 'Port Services'
            when lower(shiptype) like '%towing%'           then 'Port Services'
            when lower(shiptype) like '%pilot%'            then 'Port Services'
            when lower(shiptype) like '%anti-pollution%'   then 'Port Services'
            when lower(shiptype) like '%port tender%'      then 'Port Services'

            -- Emergency
            when lower(shiptype) like '%law enforcement%'  then 'Emergency Vessels'
            when lower(shiptype) like '%search and rescue%' then 'Emergency Vessels'
            when lower(shiptype) like '%military%'         then 'Emergency Vessels'
            
            -- Catch-all
            else 'Other'
        end as shiptype_category
    from codes
)

select * from categorized