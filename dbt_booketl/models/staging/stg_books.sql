with source as (
    select * from {{ source('raw', 'raw_books') }}
),
cleaned as (
    select
        trim(title)                   as title,
        cast(price as decimal(8,2))   as price,
        cast(rating as integer)       as rating,
        in_stock,
        initcap(trim(category))       as category,
        url,
        price_band,
        is_highly_rated,
        try_to_timestamp(scraped_at)  as scraped_at
    from source
    where title is not null
      and price > 0
)
select * from cleaned