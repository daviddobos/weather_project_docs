WITH s_fct_astro_forecast AS (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_astro_forecast"
)

SELECT * FROM s_fct_astro_forecast