
  
    
    
    USE [wh_weather];
    
    

    EXEC('create view "dbt_gold"."g_fct_weather_forecast_avg__dbt_temp__dbt_tmp_vw" as WITH s_fct_weather_forecast AS (
    SELECT
        m_valid_dt
        ,weather_pk
        ,city_nm
        ,city_fk
        ,country_cd
        ,county_nm
        ,forecast_dt
        -- Group to the start of the month
--       ,CAST(YEAR(forecast_dt) AS VARCHAR) + ''-'' + RIGHT(''0'' + CAST(MONTH(forecast_dt) AS VARCHAR), 2) + ''-01'' AS month_dt
        ,temp_c_no
        ,temp_feelslike_no
        ,cloud_coverage_pct
        ,precip_mm_no
        ,humidity_pct
        ,gust_kph_no
        ,heatindex_c_no
        ,wind_kph_no
        ,windchill_c_no
    FROM "wh_weather"."dbt_silver"."s_fct_weather_forecast"
),

-- Step 1: Pre-aggregate by city, country, and m_valid_dt
daily_aggregates AS (
    SELECT
        m_valid_dt
        ,forecast_dt
        ,city_nm
        ,city_fk
        ,county_nm
        ,country_cd
        ,AVG(temp_c_no) AS temp_c_avg
        ,AVG(cloud_coverage_pct) AS cloud_coverage_avg
        ,AVG(precip_mm_no) AS precipitation_mm_avg
        ,AVG(humidity_pct) AS humidity_avg
        ,AVG(gust_kph_no) AS gust_kph_avg
        ,AVG(heatindex_c_no) AS heatindex_c_no_avg
        ,AVG(wind_kph_no) AS wind_kph_no_avg
        ,AVG(windchill_c_no) AS windchill_c_no_avg
    FROM s_fct_weather_forecast
    GROUP BY m_valid_dt, city_nm, city_fk, county_nm, country_cd, forecast_dt
),

-- Step 2: Calculate rolling averages
rolling_averages AS (
    SELECT
        city_nm
        ,city_fk
        ,county_nm
        ,country_cd
        ,m_valid_dt
        -- Rolling averages (3 days)
        ,AVG(temp_c_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS temperature_c_avg_3d

        ,AVG(cloud_coverage_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS cloud_coverage_avg_3d

        ,AVG(precipitation_mm_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS precipitation_mm_avg_3d

        ,AVG(humidity_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS humidity_avg_3d

        ,AVG(gust_kph_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS gust_kph_avg_3d

        ,AVG(heatindex_c_no_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS heatindex_c_no_avg_3d

        ,AVG(wind_kph_no_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS wind_kph_no_avg_3d

        ,AVG(windchill_c_no_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS windchill_c_no_avg_3d
-- Rolling averages (1 week = 7 days)
        ,AVG(temp_c_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS temperature_c_avg_1w

        ,AVG(cloud_coverage_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS cloud_coverage_avg_1w

        ,AVG(precipitation_mm_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS precipitation_mm_avg_1w

        ,AVG(humidity_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS humidity_avg_1w

        ,AVG(gust_kph_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS gust_kph_avg_1w

        ,AVG(heatindex_c_no_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS heatindex_c_no_avg_1w

        ,AVG(wind_kph_no_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS wind_kph_no_avg_1w

        ,AVG(windchill_c_no_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS windchill_c_no_avg_1w
-- Rolling averages (2 weeks = 14 days)
        ,AVG(temp_c_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS temperature_c_avg_2w

        ,AVG(cloud_coverage_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS cloud_coverage_avg_2w

        ,AVG(precipitation_mm_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS precipitation_mm_avg_2w

        ,AVG(humidity_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS humidity_avg_2w

        ,AVG(gust_kph_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS gust_kph_avg_2w

        ,AVG(heatindex_c_no_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS heatindex_c_no_avg_2w

        ,AVG(wind_kph_no_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS wind_kph_no_avg_2w

        ,AVG(windchill_c_no_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS windchill_c_no_avg_2w
    FROM daily_aggregates
),

unit_conversion AS (
    SELECT
        m_valid_dt
        ,city_nm
        ,city_fk
        ,county_nm
        ,country_cd
        ,
    
    

    temperature_c_avg_3d
    , temperature_c_avg_3d * 9/5 + 32 AS temperature_f_avg_3d
    , temperature_c_avg_3d + 273.15 AS temperature_k_avg_3d

        ,
    
    

    temperature_c_avg_1w
    , temperature_c_avg_1w * 9/5 + 32 AS temperature_f_avg_1w
    , temperature_c_avg_1w + 273.15 AS temperature_k_avg_1w

        ,
    
    

    temperature_c_avg_2w
    , temperature_c_avg_2w * 9/5 + 32 AS temperature_f_avg_2w
    , temperature_c_avg_2w + 273.15 AS temperature_k_avg_2w

        ,cloud_coverage_avg_3d
        ,cloud_coverage_avg_1w
        ,cloud_coverage_avg_2w
        ,precipitation_mm_avg_3d
        ,precipitation_mm_avg_1w
        ,precipitation_mm_avg_2w
        ,humidity_avg_3d
        ,humidity_avg_1w
        ,humidity_avg_2w
        ,
    
    

    gust_kph_avg_3d
    , gust_kph_avg_3d * 0.621371 AS gust_miph_avg_3d

        ,
    
    

    gust_kph_avg_1w
    , gust_kph_avg_1w * 0.621371 AS gust_miph_avg_1w

        ,
    
    

    gust_kph_avg_2w
    , gust_kph_avg_2w * 0.621371 AS gust_miph_avg_2w

        ,
    
    

    heatindex_c_no_avg_3d
    , heatindex_c_no_avg_3d * 9/5 + 32 AS heatindex_f_no_avg_3d
    , heatindex_c_no_avg_3d + 273.15 AS heatindex_k_no_avg_3d

        ,
    
    

    heatindex_c_no_avg_1w
    , heatindex_c_no_avg_1w * 9/5 + 32 AS heatindex_f_no_avg_1w
    , heatindex_c_no_avg_1w + 273.15 AS heatindex_k_no_avg_1w

        ,
    
    

    heatindex_c_no_avg_2w
    , heatindex_c_no_avg_2w * 9/5 + 32 AS heatindex_f_no_avg_2w
    , heatindex_c_no_avg_2w + 273.15 AS heatindex_k_no_avg_2w

        ,
    
    

    wind_kph_no_avg_3d
    , wind_kph_no_avg_3d * 0.621371 AS wind_miph_no_avg_3d

        ,
    
    

    wind_kph_no_avg_1w
    , wind_kph_no_avg_1w * 0.621371 AS wind_miph_no_avg_1w

        ,
    
    

    wind_kph_no_avg_2w
    , wind_kph_no_avg_2w * 0.621371 AS wind_miph_no_avg_2w

        ,
    
    

    windchill_c_no_avg_3d
    , windchill_c_no_avg_3d * 9/5 + 32 AS windchill_f_no_avg_3d
    , windchill_c_no_avg_3d + 273.15 AS windchill_k_no_avg_3d

        ,
    
    

    windchill_c_no_avg_1w
    , windchill_c_no_avg_1w * 9/5 + 32 AS windchill_f_no_avg_1w
    , windchill_c_no_avg_1w + 273.15 AS windchill_k_no_avg_1w

        ,
    
    

    windchill_c_no_avg_2w
    , windchill_c_no_avg_2w * 9/5 + 32 AS windchill_f_no_avg_2w
    , windchill_c_no_avg_2w + 273.15 AS windchill_k_no_avg_2w

    FROM rolling_averages
)

SELECT * FROM unit_conversion;');




    
    EXEC('CREATE TABLE "wh_weather"."dbt_gold"."g_fct_weather_forecast_avg__dbt_temp" AS SELECT * FROM "wh_weather"."dbt_gold"."g_fct_weather_forecast_avg__dbt_temp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-fabric-dw'');
');
    

  
  