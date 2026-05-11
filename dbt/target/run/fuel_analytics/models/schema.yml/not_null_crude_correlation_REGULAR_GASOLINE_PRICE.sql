
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select REGULAR_GASOLINE_PRICE
from USER_DB_BOA.dbt.crude_correlation
where REGULAR_GASOLINE_PRICE is null



  
  
      
    ) dbt_internal_test