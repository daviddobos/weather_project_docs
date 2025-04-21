
    with validation as (

        -- Find all records in the child model where the foreign key does not exist in the parent model
        select child.*
        from "wh_weather"."dbt_silver"."s_fct_astro_forecast" as child
        left join "wh_weather"."dbt_silver"."s_dim_city" as parent
        on child.city_fk = parent.city_pk
        where parent.city_pk is null and child.city_fk is not null
    )

    -- Return all records from the child model that have unmatched foreign keys
    select *
    from validation

