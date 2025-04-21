WITH s_dim_country AS (
    SELECT 
        *
    FROM "wh_weather"."dbt_silver"."s_dim_country"
),

reorder AS (
    SELECT 
        country_cd
        ,country_nm
        ,continent_nm
        ,eu_member_flg
        ,currency_cd
        ,iso_alpha_3_cd
    FROM s_dim_country
)

SELECT * FROM reorder