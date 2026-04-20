-- Pre-computes geography objects from the seed's WKT strings.
-- Simplifies the large territorial polygon once here, avoiding
-- expensive recomputation in downstream spatial joins.

{{ config(materialized='table') }}

select
    zone_name,
    zone_type,
    case
        when zone_type = 'territorial'
        then st_simplify(st_geogfromtext(wkt), 100)
        else st_geogfromtext(wkt)
    end as geography
from {{ ref('piraeus_geography') }}