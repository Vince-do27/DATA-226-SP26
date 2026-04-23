select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select week_start
from USER_DB_BOA.analytics.weather_weekly_precipitation
where week_start is null



      
    ) dbt_internal_test