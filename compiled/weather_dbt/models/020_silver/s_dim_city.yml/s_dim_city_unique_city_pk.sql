
    with validation as (
        -- Find all values of the column that appear more than once
        select city_pk
        from "wh_weather"."dbt_silver"."s_dim_city"
        group by city_pk
        having count(*) > 1
    )

    -- Return the full records for rows that have duplicate values
    select *
    from "wh_weather"."dbt_silver"."s_dim_city"
    where city_pk in (select city_pk from validation)

