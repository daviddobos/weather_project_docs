

with meet_condition as(
  select *
  from "wh_weather"."dbt_silver"."s_fct_weather"
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not m_valid_dt >= CAST(DATEFROMPARTS(YEAR(GETDATE()), 1, 1) AS DATE)
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not m_valid_dt <= CAST(DATEFROMPARTS(YEAR(GETDATE()), 12, 31) AS DATE)
)

select *
from validation_errors

