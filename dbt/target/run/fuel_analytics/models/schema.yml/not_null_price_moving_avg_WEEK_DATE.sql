
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select WEEK_DATE
from USER_DB_BOA.dbt.price_moving_avg
where WEEK_DATE is null



  
  
      
    ) dbt_internal_test