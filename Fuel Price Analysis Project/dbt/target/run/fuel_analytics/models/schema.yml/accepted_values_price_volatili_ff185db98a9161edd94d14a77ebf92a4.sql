
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        VOLATILITY_LABEL as value_field,
        count(*) as n_records

    from USER_DB_BOA.dbt.price_volatility
    group by VOLATILITY_LABEL

)

select *
from all_values
where value_field not in (
    'High Volatility','Moderate Volatility','Low Volatility'
)



  
  
      
    ) dbt_internal_test