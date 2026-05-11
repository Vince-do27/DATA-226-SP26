
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        PRICE_CATEGORY as value_field,
        count(*) as n_records

    from USER_DB_BOA.dbt.regional_comparison
    group by PRICE_CATEGORY

)

select *
from all_values
where value_field not in (
    'Significantly Above Average','Above Average','Near National Average','Below Average','Significantly Below Average'
)



  
  
      
    ) dbt_internal_test