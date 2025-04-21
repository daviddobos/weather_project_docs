WITH b_astro AS (
    SELECT 
        *
    FROM "wh_weather"."dbt_bronze"."b_astro"
),

s_dim_country AS (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_dim_country"
),

s_dim_city AS (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_dim_city"
),

join_country_cd AS (
    SELECT
        b_astro.*
        ,s_dim_country.country_cd AS country_cd
    FROM b_astro
    JOIN s_dim_country
    ON b_astro.country_nm = s_dim_country.country_nm
),


gen_sk AS (
    SELECT
        *
        ,lower(convert(varchar(32), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(m_valid_dt as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(forecast_dt as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(country_cd as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(city_nm as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
 AS astro_pk
        ,lower(convert(varchar(32), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(country_cd as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(city_nm as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
 AS city_fk
    FROM join_country_cd
),

join_county_nm AS (
    SELECT
        gen_sk.*
        ,s_dim_city.county_nm
    FROM gen_sk
    JOIN s_dim_city
    ON gen_sk.city_fk = s_dim_city.city_pk
),

missing_astro_times AS (
    SELECT
        m_valid_dt
        ,astro_pk
        ,forecast_dt
        ,city_nm
        ,city_fk
        ,country_cd
        ,country_nm
        ,county_nm
        ,moon_illumination_pct
        ,moon_phase_cd
        ,CASE 
            WHEN moonrise_t = 'No moonrise' THEN NULL 
            ELSE moonrise_t
        END AS moonrise_t
        ,CASE 
            WHEN moonset_t = 'No moonset' THEN NULL 
            ELSE moonset_t
        END AS moonset_t
        ,CASE 
            WHEN sunrise_t = 'No sunrise' THEN NULL 
            ELSE sunrise_t
        END AS sunrise_t
        ,CASE 
            WHEN sunset_t = 'No sunset' THEN NULL 
            ELSE sunset_t
        END AS sunset_t
        ,m_extracted_at_dttm
        ,m_updated_at_dttm
    FROM join_county_nm
),

reorder AS (
    SELECT
        m_valid_dt
        ,astro_pk
        ,forecast_dt
        ,city_nm
        ,city_fk
        ,country_cd
        ,country_nm
        ,county_nm
        ,moon_illumination_pct
        ,moon_phase_cd
        ,CONVERT(TIME(0), moonrise_t) AS moonrise_t
        ,CONVERT(TIME(0), moonset_t) AS moonset_t
        ,CONVERT(TIME(0), sunrise_t) AS sunrise_t
        ,CONVERT(TIME(0), sunset_t) AS sunset_t
        ,m_extracted_at_dttm
        ,m_updated_at_dttm
    FROM missing_astro_times
)

SELECT * FROM reorder