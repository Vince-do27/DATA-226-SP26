
    
    

with all_values as (

    select
        MARGIN_CATEGORY as value_field,
        count(*) as n_records

    from USER_DB_BOA.dbt.crude_correlation
    group by MARGIN_CATEGORY

)

select *
from all_values
where value_field not in (
    'Wide Margin','Normal Margin','Tight Margin'
)


