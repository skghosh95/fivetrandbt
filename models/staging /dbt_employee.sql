with final as (
    select
        name  as name,
          id as id  
     
from {{ source('src','raw_employee') }}
)
select * from final
