{# SELECT 
    COALESCE(gender.gender_key, 'unset') AS gender_key,
    COALESCE(age_group.age_group_key, 'unset') AS age_group_key,
    COALESCE(epi_week.epi_week_key, 'unset') AS epi_week_key,
    COALESCE(facility.facility_key, 'unset') AS facility_key,
    COALESCE(date.date_key, 'unset') AS date_key,
    COALESCE(case_classification.case_classification_key, 'unset') AS case_classification_key,
    COUNT(DISTINCT enroll.PID) AS no_of_cases,
    CAST(CURRENT_DATE AS DATE) AS load_date,
    enroll.screening_date,
    enroll.screeningpoint
FROM {{ ref('stg_afi_surveillance') }} AS enroll

-- Joining case classification dimension
LEFT JOIN {{ ref('dim_case_classification') }} AS case_classification 
    ON case_classification.case_classification = enroll.proposed_combined_case

-- Joining gender dimension
LEFT JOIN {{ ref('dim_gender') }} AS gender 
    ON gender.code = enroll.gender

-- Joining epidemiological week dimension
LEFT JOIN {{ ref('dim_epi_week') }} AS epi_week 
    ON enroll.screening_date BETWEEN epi_week.start_of_week AND epi_week.end_of_week 

-- Joining age group dimension
LEFT JOIN {{ ref('dim_age_group_afi') }} AS age_group 
    ON enroll.calculated_age_days >= age_group.start_age_days 
    AND enroll.calculated_age_days < age_group.end_age_days

-- Joining facility dimension
LEFT JOIN {{ ref('dim_facility') }} AS facility 
    ON facility.mfl_code = enroll.site 

-- Joining date dimension
LEFT JOIN {{ ref('dim_date') }} AS date 
    ON date.date = enroll.screening_date 

WHERE enroll.eligible = 1  -- only include eligible cases
    AND enroll.consent = 1 -- only include cases with consent(the enrolled cases)

GROUP BY 
    COALESCE(gender.gender_key, 'unset'),
    COALESCE(age_group.age_group_key, 'unset'),
    COALESCE(epi_week.epi_week_key, 'unset'),
    COALESCE(facility.facility_key, 'unset'),
    COALESCE(date.date_key, 'unset'),
    COALESCE(case_classification.case_classification_key, 'unset'),
    screening_date,
    screeningpoint
 #}

 {{ config(
    materialized='table',
    post_hook=[
      "create index if not exists idx_{{ this.identifier }}__date_key on {{ this }} (date_key)",
      "create index if not exists idx_{{ this.identifier }}__epi_week_key on {{ this }} (epi_week_key)",
      "create index if not exists idx_{{ this.identifier }}__facility_key on {{ this }} (facility_key)",
      "create index if not exists idx_{{ this.identifier }}__case_class_key on {{ this }} (case_classification_key)",
      "create index if not exists idx_{{ this.identifier }}__gender_key on {{ this }} (gender_key)",
      "create index if not exists idx_{{ this.identifier }}__age_group_key on {{ this }} (age_group_key)",
      "create index if not exists idx_{{ this.identifier }}__facility_date_caseclass on {{ this }} (facility_key, date_key, case_classification_key)",
      "create index if not exists idx_{{ this.identifier }}__epi_week_facility on {{ this }} (epi_week_key, facility_key)"
    ]
) }}

with base as (
    select
        pid,
        screening_date::date as screening_date,
        screeningpoint,
        cast(gender as integer) as gender_code,     -- gender is 1,2,null
        calculated_age_days,
        site::int as mflcode,
        proposed_combined_case,
        eligible,
        consent
    from {{ ref('stg_afi_surveillance') }}
    where eligible = 1
      and consent = 1
),

joined as (
    select
        coalesce(g.gender_key, 'unset') as gender_key,
        coalesce(ag.age_group_key, 'unset') as age_group_key,
        coalesce(ew.epi_week_key, 'unset') as epi_week_key,             -- optimized via bridge
        coalesce(f.facility_key, 'unset') as facility_key,
        coalesce(d.date_key, 'unset') as date_key,
        coalesce(cc.case_classification_key, 'unset') as case_classification_key,
        b.pid,
        b.screening_date,
        b.screeningpoint
    from base b
    left join {{ ref('dim_case_classification') }} cc
      on cc.case_classification = b.proposed_combined_case
    left join {{ ref('dim_gender') }} g
      on g.code = b.gender_code
    left join {{ ref('dim_age_group_afi') }} ag
      on b.calculated_age_days >= ag.start_age_days
     and b.calculated_age_days <  ag.end_age_days
    left join {{ ref('dim_facility') }} f
      on f.mfl_code = b.mflcode
    left join {{ ref('dim_date') }} d
      on d.date = b.screening_date
    left join {{ ref('dim_date_epi_week') }} ew
      on ew.date = b.screening_date
)

select
    gender_key,
    age_group_key,
    epi_week_key,
    facility_key,
    date_key,
    case_classification_key,
    count(distinct pid) as no_of_cases,
    cast(current_date as date) as load_date,
    screening_date,
    screeningpoint
from joined
group by
    gender_key,
    age_group_key,
    epi_week_key,
    facility_key,
    date_key,
    case_classification_key,
    screening_date,
    screeningpoint
