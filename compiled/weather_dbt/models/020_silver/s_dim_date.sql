WITH b_date AS (
    SELECT 
        *
    FROM "wh_weather"."dbt_bronze"."b_date"
),

reorder AS (
    SELECT 
        date
        ,date_dttm
        ,year_num
        ,year_txt
        ,quarter_num
        ,quarter_txt
        ,month_num
        ,month_txt
        ,day_num
        ,day_txt
        ,year_quarter_txt
        ,year_month_txt
    FROM b_date
)

SELECT * FROM reorder