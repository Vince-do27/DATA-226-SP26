
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select TICKER
from USER_DB_BOA.RAW.energy_market_prices
where TICKER is null



  
  
      
    ) dbt_internal_test