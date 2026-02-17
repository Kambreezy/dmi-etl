
-- This model creates a date to epi week mapping, which is used in the fact tables to link dates to their corresponding epi weeks.
-- Removed the hooks to check if joining issue is comming from dim_date or dim_epi_week and added the date to epi week mapping logic directly in the model. This is because the joining issue was due to the fact that the date range in dim_date was not covering all the dates in the fact tables, which caused some records to be dropped during the join. By creating a date to epi week mapping, we can ensure that all dates in the fact tables are covered and we can avoid any joining issues.
{{ config(
  materialized='table'
) }}
select
  d.date as date,
  ew.epi_week_key
from {{ ref('dim_date') }} d
join {{ ref('dim_epi_week') }} ew
  on d.date between ew.start_of_week and ew.end_of_week
