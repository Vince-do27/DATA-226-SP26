
    
    

with all_values as (

    select
        REGION as value_field,
        count(*) as n_records

    from USER_DB_BOA.RAW.regional_fuel_prices
    group by REGION

)

select *
from all_values
where value_field not in (
    'EAST_COAST','MIDWEST','GULF_COAST','ROCKY_MOUNTAIN','WEST_COAST','CALIFORNIA'
)


