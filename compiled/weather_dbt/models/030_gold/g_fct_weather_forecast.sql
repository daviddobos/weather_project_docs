WITH s_fct_weather_forecast AS (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_weather_forecast"
)

SELECT * FROM s_fct_weather_forecast