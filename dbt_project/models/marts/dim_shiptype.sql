-- Shiptype dimension.
-- Lookup table for ship type codes, names, and categories.
-- One row per shiptype_code.

select
    shiptype_code,
    shiptype,
    shiptype_category
from {{ ref('int_shiptype_enriched') }}