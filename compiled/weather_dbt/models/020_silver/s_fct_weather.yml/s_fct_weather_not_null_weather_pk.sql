

with validation as (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_weather"
    WHERE weather_pk IS NULL
)

select * from validation

