

with validation as (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_dim_city"
    WHERE city_pk IS NULL
)

select * from validation

