WITH s_fct_weather AS (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_weather"
)

SELECT * FROM s_fct_weather