
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select REGULAR_4WK_AVG
from USER_DB_BOA.dbt.price_moving_avg
where REGULAR_4WK_AVG is null



  
  
      
    ) dbt_internal_test