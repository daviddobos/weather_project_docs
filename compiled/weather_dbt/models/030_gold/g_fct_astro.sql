WITH s_fct_astro AS (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_astro"
)

SELECT * FROM s_fct_astro