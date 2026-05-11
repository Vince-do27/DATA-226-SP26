
    
    

with all_values as (

    select
        FUEL_TYPE as value_field,
        count(*) as n_records

    from USER_DB_BOA.RAW.regional_fuel_prices
    group by FUEL_TYPE

)

select *
from all_values
where value_field not in (
    'REGULAR_GASOLINE'
)


