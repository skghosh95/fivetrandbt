select * from Raw_employee
(
    select
        name,
          id  
     
from {{ ref('Raw_employee') }}
)
