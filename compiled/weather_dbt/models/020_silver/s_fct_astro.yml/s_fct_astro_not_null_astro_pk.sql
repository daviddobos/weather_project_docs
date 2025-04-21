

with validation as (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_astro"
    WHERE astro_pk IS NULL
)

select * from validation

