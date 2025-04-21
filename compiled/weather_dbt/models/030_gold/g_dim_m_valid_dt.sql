WITH distinct_m_valid_dt AS (
    SELECT DISTINCT
        m_valid_dt
    FROM "wh_weather"."dbt_silver"."s_fct_weather"
)

SELECT * FROM distinct_m_valid_dt