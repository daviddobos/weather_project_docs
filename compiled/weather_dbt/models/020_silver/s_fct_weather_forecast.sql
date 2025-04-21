WITH b_weather_forecast AS (
    SELECT 
        *
    FROM "wh_weather"."dbt_bronze"."b_weather_forecast"
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
        b_weather_forecast.*
        ,s_dim_country.country_cd AS country_cd
    FROM b_weather_forecast
    JOIN s_dim_country
    ON b_weather_forecast.country_nm = s_dim_country.country_nm
),

gen_sk AS (
    SELECT
        *
        ,lower(convert(varchar(32), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(m_valid_dt as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(forecast_dt as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(country_cd as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(city_nm as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(time_epoch as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
 AS weather_pk
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

unit_conversion AS (
    SELECT
        *
        ,gust_kph_no * 0.621371 AS gust_miph_no
        ,temp_c_no * 33.8 AS temp_f_no
        ,temp_c_no * 273.15 AS temp_k_no
        ,heatindex_c_no * 33.8 AS heatindex_f_no
        ,heatindex_c_no * 273.15 AS heatindex_k_no
        ,vis_km_no * 0.621371 AS vis_mi_no
        ,wind_kph_no * 0.621371 AS wind_miph_no
        ,windchill_c_no * 33.8 AS windchill_f_no
        ,windchill_c_no * 273.15 AS windchill_k_no
    FROM join_county_nm
),

weather_categories AS (
    SELECT
        *
        -- Rain Chance Category
        ,CASE
            WHEN rain_chance_pct BETWEEN 0 AND 20 THEN 'Very Low'
            WHEN rain_chance_pct BETWEEN 21 AND 40 THEN 'Low'
            WHEN rain_chance_pct BETWEEN 41 AND 60 THEN 'Moderate'
            WHEN rain_chance_pct BETWEEN 61 AND 80 THEN 'High'
            WHEN rain_chance_pct BETWEEN 81 AND 100 THEN 'Very High'
            ELSE 'Unknown'
        END AS rain_chance_cd

        -- Snow Chance Category
        ,CASE
            WHEN snow_chance_pct BETWEEN 0 AND 20 THEN 'Very Low'
            WHEN snow_chance_pct BETWEEN 21 AND 40 THEN 'Low'
            WHEN snow_chance_pct BETWEEN 41 AND 60 THEN 'Moderate'
            WHEN snow_chance_pct BETWEEN 61 AND 80 THEN 'High'
            WHEN snow_chance_pct BETWEEN 81 AND 100 THEN 'Very High'
            ELSE 'Unknown'
        END AS snow_chance_cd

        -- Cloud Coverage Category
        ,CASE
            WHEN cloud_coverage_pct BETWEEN 0 AND 20 THEN 'Clear'
            WHEN cloud_coverage_pct BETWEEN 21 AND 40 THEN 'Partly Cloudy'
            WHEN cloud_coverage_pct BETWEEN 41 AND 60 THEN 'Cloudy'
            WHEN cloud_coverage_pct BETWEEN 61 AND 80 THEN 'Overcast'
            WHEN cloud_coverage_pct BETWEEN 81 AND 100 THEN 'Completely Overcast'
            ELSE 'Unknown'
        END AS cloud_coverage_cd

        -- Feels Like Temperature Category
        ,CASE
            WHEN temp_feelslike_no <= 0 THEN 'Very Cold'
            WHEN temp_feelslike_no BETWEEN 1 AND 10 THEN 'Cold'
            WHEN temp_feelslike_no BETWEEN 11 AND 20 THEN 'Cool'
            WHEN temp_feelslike_no BETWEEN 21 AND 30 THEN 'Warm'
            WHEN temp_feelslike_no BETWEEN 31 AND 40 THEN 'Hot'
            WHEN temp_feelslike_no > 40 THEN 'Very Hot'
            ELSE 'Unknown'
        END AS temp_feelslike_cd

        -- Humidity Category
        ,CASE
            WHEN humidity_pct BETWEEN 0 AND 20 THEN 'Very Low'
            WHEN humidity_pct BETWEEN 21 AND 40 THEN 'Low'
            WHEN humidity_pct BETWEEN 41 AND 60 THEN 'Moderate'
            WHEN humidity_pct BETWEEN 61 AND 80 THEN 'High'
            WHEN humidity_pct BETWEEN 81 AND 100 THEN 'Very High'
            ELSE 'Unknown'
        END AS humidity_cd

    FROM unit_conversion
),

reorder AS (
    SELECT
        m_valid_dt
        ,weather_pk
        ,forecast_dt
        ,city_nm
        ,city_fk
        ,country_cd
        ,country_nm
        ,county_nm
        ,cloud_coverage_pct
        ,cloud_coverage_cd
        ,gust_kph_no
        ,gust_miph_no
        ,heatindex_c_no
        ,heatindex_f_no
        ,heatindex_k_no
        ,humidity_pct
        ,humidity_cd
        ,is_day_flg
        ,precip_mm_no
        ,pressure_mb_no
        ,temp_c_no
        ,temp_f_no
        ,temp_k_no
        ,temp_feelslike_no
        ,temp_feelslike_cd
        ,date_dtt
        ,time_epoch
        ,rain_chance_pct
        ,rain_chance_cd
        ,snow_chance_pct
        ,snow_chance_cd
        ,uv_no
        ,vis_km_no
        ,vis_mi_no
        ,wind_dir_cd
        ,wind_kph_no
        ,wind_miph_no
        ,windchill_c_no
        ,windchill_f_no
        ,windchill_k_no
        ,m_extracted_at_dttm
        ,m_updated_at_dttm
    FROM weather_categories
)

SELECT * FROM reorder