select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select weekly_precipitation
from USER_DB_BOA.analytics.weather_weekly_precipitation
where weekly_precipitation is null



      
    ) dbt_internal_test