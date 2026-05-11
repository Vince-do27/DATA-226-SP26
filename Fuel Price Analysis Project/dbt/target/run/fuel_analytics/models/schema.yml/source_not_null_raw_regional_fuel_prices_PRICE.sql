
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from USER_DB_BOA.RAW.regional_fuel_prices
where PRICE is null



  
  
      
    ) dbt_internal_test