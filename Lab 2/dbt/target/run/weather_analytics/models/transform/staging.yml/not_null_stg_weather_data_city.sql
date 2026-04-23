select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select city
from USER_DB_BOA.analytics.stg_weather_data
where city is null



      
    ) dbt_internal_test