select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select days_recorded
from USER_DB_BOA.analytics.percent_rainy_days_monthly
where days_recorded is null



      
    ) dbt_internal_test