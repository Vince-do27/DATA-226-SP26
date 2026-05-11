
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        TICKER as value_field,
        count(*) as n_records

    from USER_DB_BOA.RAW.energy_market_prices
    group by TICKER

)

select *
from all_values
where value_field not in (
    'CL=F','BZ=F','XLE','UGA'
)



  
  
      
    ) dbt_internal_test