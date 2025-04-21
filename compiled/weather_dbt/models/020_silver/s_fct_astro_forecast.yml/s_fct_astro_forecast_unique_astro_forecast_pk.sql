
    with validation as (
        -- Find all values of the column that appear more than once
        select astro_forecast_pk
        from "wh_weather"."dbt_silver"."s_fct_astro_forecast"
        group by astro_forecast_pk
        having count(*) > 1
    )

    -- Return the full records for rows that have duplicate values
    select *
    from "wh_weather"."dbt_silver"."s_fct_astro_forecast"
    where astro_forecast_pk in (select astro_forecast_pk from validation)

