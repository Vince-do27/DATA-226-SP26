select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select precipitation_sum
from USER_DB_BOA.analytics.stg_weather_data
where precipitation_sum is null



      
    ) dbt_internal_test