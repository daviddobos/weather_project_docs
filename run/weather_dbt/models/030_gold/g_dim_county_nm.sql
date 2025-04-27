
  
    
    
    USE [wh_weather];
    
    

    EXEC('create view "dbt_gold"."g_dim_county_nm__dbt_temp__dbt_tmp_vw" as WITH s_fct_astro AS (
    SELECT DISTINCT
        county_nm
    FROM "wh_weather"."dbt_silver"."s_fct_astro"
),

s_fct_astro_forecast AS (
    SELECT DISTINCT
        county_nm
    FROM "wh_weather"."dbt_silver"."s_fct_astro_forecast"
),

s_fct_weather AS (
    SELECT DISTINCT
        county_nm
    FROM "wh_weather"."dbt_silver"."s_fct_weather"
),

s_fct_weather_forecast AS (
    SELECT DISTINCT
        county_nm
    FROM "wh_weather"."dbt_silver"."s_fct_weather_forecast"
),

union_select AS (
    SELECT * FROM s_fct_astro
    UNION
    SELECT * FROM s_fct_astro_forecast
    UNION
    SELECT * FROM s_fct_weather
    UNION
    SELECT * FROM s_fct_weather_forecast
)

SELECT * FROM union_select;');




    
    EXEC('CREATE TABLE "wh_weather"."dbt_gold"."g_dim_county_nm__dbt_temp" AS SELECT * FROM "wh_weather"."dbt_gold"."g_dim_county_nm__dbt_temp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-fabric-dw'');
');
    

  
  