

with validation as (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_astro_forecast"
    WHERE astro_forecast_pk IS NULL
)

select * from validation

