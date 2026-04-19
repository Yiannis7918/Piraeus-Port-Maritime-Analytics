with source as (
    select * from {{ source('piraeus_raw', 'ais_codes_descriptions') }}
),

cleaned as (
    select
        `Type Code` as shiptype_code,
        Description as shiptype_name
    from source
    where `Type Code` is not null
)

select * from cleaned