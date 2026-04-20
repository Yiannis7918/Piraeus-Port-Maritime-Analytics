-- Date dimension covering the Piraeus AIS dataset period.
-- One row per calendar date from 2017-01-01 through 2019-12-31.
-- Includes calendar attributes (year, month, week, season) plus
-- flags for analytical conveniences:
--   - is_complete_year: true for 2018, 2019 (full data coverage)
--   - is_in_dataset: true for dates where our AIS data actually exists

with date_spine as (
    select date_day
    from unnest(
        generate_date_array(
            date '2017-01-01',
            date '2019-12-31',
            interval 1 day
        )
    ) as date_day
),

date_attributes as (
    select
        date_day as date_key,
        
        -- Time hierarchy
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        format_date('%B', date_day) as month_name,
        extract(isoweek from date_day) as week,
        extract(dayofweek from date_day) as day_of_week,
        format_date('%A', date_day) as day_name,
        
        -- Season (meteorological)
        case
            when extract(month from date_day) in (12, 1, 2) then 'Winter'
            when extract(month from date_day) in (3, 4, 5) then 'Spring'
            when extract(month from date_day) in (6, 7, 8) then 'Summer'
            when extract(month from date_day) in (9, 10, 11) then 'Autumn'
        end as season,
        
        -- Weekend flag
        case
            when extract(dayofweek from date_day) in (1, 7) then true
            else false
        end as is_weekend,
        
        -- Data completeness flags for the Piraeus AIS dataset
        case
            when extract(year from date_day) in (2018, 2019) then true
            else false
        end as is_complete_year,
        
        -- True if the date falls within the actual AIS collection period
        -- (May 9, 2017 through December 26, 2019)
        case
            when date_day between date '2017-05-09' and date '2019-12-26' then true
            else false
        end as is_in_dataset
        
    from date_spine
)

select * from date_attributes