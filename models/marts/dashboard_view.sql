-- Dashboard-ready view for Looker Studio.
-- Joins fact_piraeus_activity_daily with dim_date.
-- Single source for the dashboard to simplify filter and aggregation logic.

{{ config(materialized='view') }}

select
    f.event_date,
    f.shiptype_category,
    f.zone,
    f.distinct_vessels,
    f.ping_count,
    f.distinct_countries,
    f.avg_speed,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week,
    d.day_of_week,
    d.day_name,
    d.season,
    d.is_weekend,
    d.is_complete_year,
    d.is_in_dataset
from {{ ref('fact_piraeus_activity_daily') }} f
left join {{ ref('dim_date') }} d
    on f.event_date = d.date_key