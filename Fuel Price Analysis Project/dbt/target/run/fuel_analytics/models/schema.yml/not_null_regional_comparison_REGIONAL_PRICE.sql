
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select REGIONAL_PRICE
from USER_DB_BOA.dbt.regional_comparison
where REGIONAL_PRICE is null



  
  
      
    ) dbt_internal_test