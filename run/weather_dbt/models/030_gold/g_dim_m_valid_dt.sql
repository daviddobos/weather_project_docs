
  
    
    
    USE [wh_weather];
    
    

    EXEC('create view "dbt_gold"."g_dim_m_valid_dt__dbt_temp__dbt_tmp_vw" as WITH distinct_m_valid_dt AS (
    SELECT DISTINCT
        m_valid_dt
    FROM "wh_weather"."dbt_silver"."s_fct_weather"
)

SELECT * FROM distinct_m_valid_dt;');




    
    EXEC('CREATE TABLE "wh_weather"."dbt_gold"."g_dim_m_valid_dt__dbt_temp" AS SELECT * FROM "wh_weather"."dbt_gold"."g_dim_m_valid_dt__dbt_temp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-fabric-dw'');
');
    

  
  