{{ config(
    materialized='table'
) }}

with subset_data as (

    select
        screening_date::date as screening_date,
        gender as sex,
        site::int as mflcode,
        screeningpoint,
        calculated_age_days,

        /* 0/1 flags (cast to int so sum() is clean) */
        (case when "Unique_ID" is not null then 1 else 0 end)::int as screened,
        (case when eligible = 1 and consent <> 6 then 1 else 0 end)::int as eligible,
        (case when consent = 1 then 1 else 0 end)::int as enrolled,
        (case when consent = 1 and proposed_combined_case <> 'DNS' then 1 else 0 end)::int as eligible_sampling,
        (case when consent = 1 and sampled = 1 then 1 else 0 end)::int as sampled,
        (case when eligible = 1 and consent = 2 then 1 else 0 end)::int as declined_enrollment

    from {{ ref('stg_afi_surveillance') }}

)

select
    coalesce(f.facility_key, 'unset') as facility_key,
    coalesce(d.date_key, 'unset') as date_key,
    coalesce(g.gender_key, 'unset') as gender_key,
    coalesce(ag.age_group_key, 'unset') as age_group_key,
    coalesce(sp.screeningpoint_key, 'unset') as screeningpoint_key,
    coalesce(dew.epi_week_key, 'unset') as epi_week_key,

    sum(sd.screened) as screened,
    sum(sd.eligible) as eligible,
    sum(sd.enrolled) as enrolled,
    sum(sd.eligible_sampling) as eligible_sampling,
    sum(sd.sampled) as sampled,
    sum(sd.declined_enrollment) as declined_enrollment,

    current_date::date as load_date

from subset_data sd
left join {{ ref('dim_facility') }} f
    on f.mfl_code = sd.mflcode
left join {{ ref('dim_date') }} d
    on d.date = sd.screening_date
left join {{ ref('dim_date_epi_week') }} dew
    on dew.date = sd.screening_date
left join {{ ref('dim_gender') }} g
    on g.code = sd.sex
left join {{ ref('dim_age_group_afi') }} ag
    on sd.calculated_age_days >= ag.start_age_days
   and sd.calculated_age_days <  ag.end_age_days
left join {{ ref('dim_afi_screening_point') }} sp
    on sp.screeningpoint = sd.screeningpoint

group by
    coalesce(f.facility_key, 'unset'),
    coalesce(d.date_key, 'unset'),
    coalesce(g.gender_key, 'unset'),
    coalesce(ag.age_group_key, 'unset'),
    coalesce(sp.screeningpoint_key, 'unset'),
    coalesce(dew.epi_week_key, 'unset')

