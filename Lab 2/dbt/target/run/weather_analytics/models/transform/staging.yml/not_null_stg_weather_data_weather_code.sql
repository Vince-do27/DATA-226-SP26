select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select weather_code
from USER_DB_BOA.analytics.stg_weather_data
where weather_code is null



      
    ) dbt_internal_test