
  
    
    
    USE [wh_weather];
    
    

    EXEC('create view "dbt_dq"."dq_dim_table_nm__dbt_temp__dbt_tmp_vw" as WITH dq_standard_tests AS (
    SELECT DISTINCT 
        table_nm
    FROM "wh_weather"."dbt_dq"."dq_standard_tests"
),

dq_business_tests AS (
    SELECT DISTINCT 
        table_nm
    FROM "wh_weather"."dbt_dq"."dq_business_tests"
)

SELECT * FROM dq_standard_tests
UNION
SELECT * FROM dq_business_tests;');




    
    EXEC('CREATE TABLE "wh_weather"."dbt_dq"."dq_dim_table_nm__dbt_temp" AS SELECT * FROM "wh_weather"."dbt_dq"."dq_dim_table_nm__dbt_temp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-fabric-dw'');
');
    

  
  