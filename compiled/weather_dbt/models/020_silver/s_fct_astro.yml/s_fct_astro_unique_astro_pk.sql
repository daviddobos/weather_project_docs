
    with validation as (
        -- Find all values of the column that appear more than once
        select astro_pk
        from "wh_weather"."dbt_silver"."s_fct_astro"
        group by astro_pk
        having count(*) > 1
    )

    -- Return the full records for rows that have duplicate values
    select *
    from "wh_weather"."dbt_silver"."s_fct_astro"
    where astro_pk in (select astro_pk from validation)

