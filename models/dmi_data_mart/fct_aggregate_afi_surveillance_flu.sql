{# select 
    COALESCE(gender.gender_key, 'unset') AS gender_key,
    COALESCE(age_group.age_group_key, 'unset') AS age_group_key,
    COALESCE(epi_week.epi_week_key, 'unset') AS epi_week_key,
    COALESCE(facility.facility_key, 'unset') AS facility_key,
    COALESCE(date.date_key, 'unset') AS date_key,
    coalesce(result.lab_result_key, 'unset') as lab_result_key,
    CASE when  consent=1 then 1 else 0 end as enrolled,
    site::int as mflcode
from  {{ ref('stg_afi_surveillance') }} as afi_data_flu
left join {{ ref('dim_lab_result') }} as result on result.lab_result_2 = afi_data_flu.fluresult 

-- Joining gender dimension
LEFT JOIN {{ ref('dim_gender') }} AS gender 
    ON gender.code = afi_data_flu.gender

-- Joining epidemiological week dimension
LEFT JOIN {{ ref('dim_epi_week') }} AS epi_week 
    ON afi_data_flu.screening_date BETWEEN epi_week.start_of_week AND epi_week.end_of_week 

-- Joining age group dimension
LEFT JOIN {{ ref('dim_age_group_afi') }} AS age_group 
    ON afi_data_flu.calculated_age_days >= age_group.start_age_days 
    AND afi_data_flu.calculated_age_days < age_group.end_age_days

-- Joining facility dimension
LEFT JOIN {{ ref('dim_facility') }} AS facility 
    ON facility.mfl_code = afi_data_flu.site 

-- Joining date dimension
LEFT JOIN {{ ref('dim_date') }} AS date 
    ON date.date = afi_data_flu.screening_date 
 #}
with base as (
    select
        screening_date::date as screening_date,

        -- gender is stored as 1, 2, null → normalize to the dim_gender.code you expect
        -- Option A: if dim_gender.code is integer (1/2), keep as integer
        cast(gender as integer) as gender_code,

        -- Option B: if dim_gender.code is 'M'/'F', use this instead:
        -- case
        --   when gender = 1 then 'M'
        --   when gender = 2 then 'F'
        --   else null
        -- end as gender_code,

        calculated_age_days,
        site::int as mflcode,
        fluresult,
        consent
    from {{ ref('stg_afi_surveillance') }}
)

select
    coalesce(g.gender_key, 'unset') as gender_key,
    coalesce(ag.age_group_key, 'unset') as age_group_key,
    coalesce(ew.epi_week_key, 'unset') as epi_week_key,     -- ✅ optimized via bridge
    coalesce(f.facility_key, 'unset') as facility_key,
    coalesce(d.date_key, 'unset') as date_key,
    coalesce(lr.lab_result_key, 'unset') as lab_result_key,

    case when b.consent = 1 then 1 else 0 end as enrolled,
    b.mflcode

from base b

left join {{ ref('dim_lab_result') }} lr
    on lr.lab_result_2 = b.fluresult

left join {{ ref('dim_gender') }} g
    on g.code = b.gender_code

left join {{ ref('dim_age_group_afi') }} ag
    on b.calculated_age_days >= ag.start_age_days
   and b.calculated_age_days <  ag.end_age_days

left join {{ ref('dim_facility') }} f
    on f.mfl_code = b.mflcode

left join {{ ref('dim_date') }} d
    on d.date = b.screening_date

-- ✅ optimized epi-week join
left join {{ ref('dim_date_epi_week') }} ew
    on ew.date = b.screening_date


