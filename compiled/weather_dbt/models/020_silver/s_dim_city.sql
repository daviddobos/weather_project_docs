WITH b_city AS (
    SELECT 
        *
    FROM "wh_weather"."dbt_bronze"."b_city"
),

city_detailed AS (
    SELECT
        city_nm
        ,ksh_cd
        ,city_type_cd
        ,county_nm
        ,district_cd
        ,district_nm
        ,district_seat_nm
        ,area_no
        ,population_no
        ,apartments_no
        ,country_cd
    FROM "wh_weather"."dbt"."city"  
),

join_detailed AS (
    SELECT
        city_detailed.*
    FROM b_city
    JOIN city_detailed
    ON b_city.city_nm = city_detailed.city_nm
    AND b_city.country_cd = city_detailed.country_cd
    AND b_city.county_nm = city_detailed.county_nm
),

gen_sk AS (
    SELECT
        *
        ,lower(convert(varchar(32), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(country_cd as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(city_nm as VARCHAR(64)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
 AS city_pk   --county_cd needs to be added to weather tables then here
    FROM join_detailed
),

reorder AS (
    SELECT
        city_pk
        ,country_cd
        ,county_nm
        ,city_nm
        ,ksh_cd
        ,city_type_cd
        ,district_cd
        ,district_nm
        ,district_seat_nm
        ,area_no
        ,population_no
        ,apartments_no
    FROM gen_sk
)

SELECT * FROM reorder