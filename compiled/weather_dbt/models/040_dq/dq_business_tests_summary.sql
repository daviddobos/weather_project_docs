WITH dq_business_tests AS (
    SELECT
        *
    FROM "wh_weather"."dbt_dq"."dq_business_tests"
),

unique_tests AS (
    SELECT
        m_valid_dt
        ,table_nm
        ,test_type_cd
        ,tested_field_nm
        ,SUM(failed_record_cnt) AS failed_records_cnt
    FROM dq_business_tests
    GROUP BY m_valid_dt, table_nm, test_type_cd, tested_field_nm
),

summary AS (
    SELECT
        m_valid_dt
        ,test_type_cd
        ,COUNT(*) AS test_cnt 
        ,SUM(CASE WHEN failed_records_cnt > 0 THEN 1 ELSE 0 END) AS failed_test_cnt
        ,SUM(CASE WHEN failed_records_cnt = 0 THEN 1 ELSE 0 END) AS passed_test_cnt
        ,SUM(failed_records_cnt) AS failed_records_cnt 
    FROM unique_tests
    GROUP BY m_valid_dt, test_type_cd
)
SELECT * 
FROM summary