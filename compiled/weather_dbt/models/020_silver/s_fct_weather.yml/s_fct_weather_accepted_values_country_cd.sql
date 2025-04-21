

with validation as (
    select *
    from "wh_weather"."dbt_silver"."s_fct_weather"
    where country_cd not in 
        (

        
            'HU'
        
   )
)

select * from validation

