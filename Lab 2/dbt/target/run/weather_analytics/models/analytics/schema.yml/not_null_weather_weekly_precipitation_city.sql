select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select city
from USER_DB_BOA.analytics.weather_weekly_precipitation
where city is null



      
    ) dbt_internal_test