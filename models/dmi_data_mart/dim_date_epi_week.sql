
-- This model creates a date to epi week mapping, which is used in the fact tables to link dates to their corresponding epi weeks.
{{ config(
  materialized='table'
) }}
select
  d.date as date,
  ew.epi_week_key
from {{ ref('dim_date') }} d
join {{ ref('dim_epi_week') }} ew
  on d.date between ew.start_of_week and ew.end_of_week
