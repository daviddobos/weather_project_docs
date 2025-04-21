
    with validation as (
        -- Find all values of the column that appear more than once
        select weather_pk
        from "wh_weather"."dbt_silver"."s_fct_weather_forecast"
        group by weather_pk
        having count(*) > 1
    )

    -- Return the full records for rows that have duplicate values
    select *
    from "wh_weather"."dbt_silver"."s_fct_weather_forecast"
    where weather_pk in (select weather_pk from validation)

