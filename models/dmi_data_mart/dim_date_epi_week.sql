{{ config(
  materialized='table',
  post_hook=[
    'create index if not exists idx_' ~ this.identifier ~ '__date on ' ~ this ~ ' (date)',
    'create index if not exists idx_' ~ this.identifier ~ '__epi_week_key on ' ~ this ~ ' (epi_week_key)'
  ]
) }}


select
  d.date as date,
  ew.epi_week_key
from {{ ref('dim_date') }} d
join {{ ref('dim_epi_week') }} ew
  on d.date between ew.start_of_week and ew.end_of_week
