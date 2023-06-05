
with final as (


SELECT
    Raw_employee.value:_name::VARCHAR as name,

    Raw_employee.value:_id::VARCHAR as id,
    from {{ source('name', 'id') }}
 )  
 select * from final
