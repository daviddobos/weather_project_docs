WITH ld_weather_forecast AS (
    SELECT 
        *
    FROM "lh_weather"."dbo"."ld_weather_forecast"
),

b_weather_forecast AS (
    SELECT
        CONVERT(DATE, m_valid_dt) AS m_valid_dt
        ,CONVERT(INT, chance_of_rain) AS rain_chance_pct
        ,CONVERT(INT, chance_of_snow) AS snow_chance_pct
        ,CONVERT(INT, cloud) AS cloud_coverage_pct
        ,CONVERT(DECIMAL(14, 4), feelslike_c) AS temp_feelslike_no
        ,CONVERT(DECIMAL(14, 4), gust_kph) AS gust_kph_no
        ,CONVERT(INT, humidity) AS humidity_pct
        ,CONVERT(DECIMAL(14, 4), heatindex_c) AS heatindex_c_no
        ,CONVERT(BIT, is_day) AS is_day_flg
        ,CONVERT(DECIMAL(14, 4), precip_mm) AS precip_mm_no
        ,CONVERT(DECIMAL(14, 4), pressure_mb) AS pressure_mb_no
        ,CONVERT(DECIMAL(14, 4), temp_c) AS temp_c_no
        ,CONVERT(DATETIME2(6), time) date_dtt
        ,CONVERT(INT, time_epoch) AS time_epoch
        ,CONVERT(DECIMAL(14, 4), uv) AS uv_no
        ,CONVERT(DECIMAL(14, 4), vis_km) AS vis_km_no
        ,CONVERT(VARCHAR(8), wind_dir) AS wind_dir_cd
        ,CONVERT(DECIMAL(14, 4), wind_kph) AS wind_kph_no
        ,CONVERT(DECIMAL(14, 4), windchill_c) AS windchill_c_no
        ,CONVERT(VARCHAR(30), city) AS city_nm
        ,CONVERT(VARCHAR(30), country_EN) AS country_nm
        ,CONVERT(DATE, forecast_date) AS forecast_dt
        ,CONVERT(DATETIME2(6), m_extracted_at_dttm) AS m_extracted_at_dttm
        ,CONVERT(DATETIME2(6), m_updated_at_dttm) AS m_updated_at_dttm
    FROM ld_weather_forecast
)

SELECT * FROM b_weather_forecast