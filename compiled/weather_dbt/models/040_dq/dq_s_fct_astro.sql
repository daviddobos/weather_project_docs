WITH s_fct_astro_not_null_astro_pk AS (
    SELECT 
        'astro_pk' AS tested_field_nm
        ,'not_null' AS test_type_cd
        ,*
    FROM "wh_weather"."dbt_dq_test"."s_fct_astro_not_null_astro_pk"
),

s_fct_astro_unique_astro_pk AS (
    SELECT 
        'astro_pk' AS tested_field_nm
        ,'unique' AS test_type_cd
        ,*
    FROM "wh_weather"."dbt_dq_test"."s_fct_astro_unique_astro_pk"
),

s_fct_astro_accepted_range_m_valid_dt AS (
    SELECT 
        'm_valid_dt' AS tested_field_nm
        ,'accepted_range' AS test_type_cd
        ,*
    FROM "wh_weather"."dbt_dq_test"."s_fct_astro_accepted_range_m_valid_dt"
),

s_fct_astro_accepted_values_country_cd AS (
    SELECT 
        'country_cd' AS tested_field_nm
        ,'accepted_values' AS test_type_cd
        ,*
    FROM "wh_weather"."dbt_dq_test"."s_fct_astro_accepted_values_country_cd"
),

s_fct_astro_rel_s_dim_city_city_fk AS (
    SELECT
        'city_fk' AS tested_field_nm
        ,'relationships' AS test_type_cd
        ,*
    FROM "wh_weather"."dbt_dq_test"."s_fct_astro_rel_s_dim_city_city_fk"  
),

union_tests AS (
    SELECT * FROM s_fct_astro_not_null_astro_pk
    UNION ALL
    SELECT * FROM s_fct_astro_unique_astro_pk
    UNION ALL
    SELECT * FROM s_fct_astro_accepted_range_m_valid_dt
    UNION ALL
    SELECT * FROM s_fct_astro_accepted_values_country_cd
    UNION ALL
    SELECT * FROM s_fct_astro_rel_s_dim_city_city_fk
)

SELECT * FROM union_tests