with books as (
    select * from {{ ref('stg_books') }}
)
select
    category,
    count(*)                                    as total_books,
    count(case when in_stock     then 1 end)    as in_stock_count,
    count(case when not in_stock then 1 end)    as out_of_stock_count,
    round(avg(price), 2)                        as avg_price,
    round(min(price), 2)                        as min_price,
    round(max(price), 2)                        as max_price,
    round(avg(rating), 2)                       as avg_rating,
    count(case when is_highly_rated then 1 end) as highly_rated_count,
    max(scraped_at)                             as last_scraped_at
from books
group by category
order by total_books desc