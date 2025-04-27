
  
    
    
    USE [wh_weather];
    
    

    EXEC('create view "dbt_gold"."g_fct_weather__dbt_temp__dbt_tmp_vw" as WITH s_fct_weather AS (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_weather"
)

SELECT * FROM s_fct_weather;');




    
    EXEC('CREATE TABLE "wh_weather"."dbt_gold"."g_fct_weather__dbt_temp" AS SELECT * FROM "wh_weather"."dbt_gold"."g_fct_weather__dbt_temp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-fabric-dw'');
');
    

  
  