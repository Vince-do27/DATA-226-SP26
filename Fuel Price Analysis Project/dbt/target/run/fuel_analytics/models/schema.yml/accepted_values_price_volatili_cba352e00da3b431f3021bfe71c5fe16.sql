
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        PRICE_DIRECTION as value_field,
        count(*) as n_records

    from USER_DB_BOA.dbt.price_volatility
    group by PRICE_DIRECTION

)

select *
from all_values
where value_field not in (
    'Rising','Falling','Stable'
)



  
  
      
    ) dbt_internal_test