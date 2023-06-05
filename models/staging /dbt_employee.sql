with final as (
    select
        name  as name,
          id as id  
     
from {{ source('src','Raw_employee') }}
)
select * from final
