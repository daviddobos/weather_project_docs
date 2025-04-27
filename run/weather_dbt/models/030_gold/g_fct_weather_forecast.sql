
  
    
    
    USE [wh_weather];
    
    

    EXEC('create view "dbt_gold"."g_fct_weather_forecast__dbt_temp__dbt_tmp_vw" as WITH s_fct_weather_forecast AS (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_weather_forecast"
)

SELECT * FROM s_fct_weather_forecast;');




    
    EXEC('CREATE TABLE "wh_weather"."dbt_gold"."g_fct_weather_forecast__dbt_temp" AS SELECT * FROM "wh_weather"."dbt_gold"."g_fct_weather_forecast__dbt_temp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-fabric-dw'');
');
    

  
  