WITH s_dim_city AS (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_dim_city"
)

SELECT * FROM s_dim_city