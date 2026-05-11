
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CLOSE_PRICE
from USER_DB_BOA.RAW.energy_market_prices
where CLOSE_PRICE is null



  
  
      
    ) dbt_internal_test