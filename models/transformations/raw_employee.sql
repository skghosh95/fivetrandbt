select * from raw_employee
(
    select
        name,
          id  
     
from {{ ref('raw_employee') }}
)
