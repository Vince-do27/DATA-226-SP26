
    
    

with all_values as (

    select
        SOURCE as value_field,
        count(*) as n_records

    from USER_DB_BOA.RAW.fuel_prices
    group by SOURCE

)

select *
from all_values
where value_field not in (
    'EIA_WEEKLY'
)


