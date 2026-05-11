
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select REGION
from USER_DB_BOA.dbt.regional_comparison
where REGION is null



  
  
      
    ) dbt_internal_test