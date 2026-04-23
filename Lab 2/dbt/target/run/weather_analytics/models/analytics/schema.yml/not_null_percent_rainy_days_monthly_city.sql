select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select city
from USER_DB_BOA.analytics.percent_rainy_days_monthly
where city is null



      
    ) dbt_internal_test