{{ config(
    materialized='table'
) }}

with base_enroll as (
    -- One-time dimension mapping for ENROLLED-related datasets (PID grain)
    select
        e."PID" as pid,
        coalesce(g.gender_key, 'unset') as gender_key,
        coalesce(ag.age_group_key, 'unset') as age_group_key,
        coalesce(ew.epi_week_key, 'unset') as epi_week_key,
        coalesce(f.facility_key, 'unset') as facility_key,
        coalesce(d.date_key, 'unset') as date_key,
        e.interview_date
    from {{ ref('stg_afi_enroll_and_household_info') }} e
    left join {{ ref('dim_gender') }} g
        on g.code = e."Gender"
    left join {{ ref('dim_age_group_afi_and_mortality') }} ag
        on e."Ageyrs" >= ag.start_age and e."Ageyrs" < ag.end_age
    left join {{ ref('dim_epi_week') }} ew
        on e.interview_date >= ew.start_of_week and e.interview_date <= ew.end_of_week
    left join {{ ref('dim_facility') }} f
        on f.code = left(e."PID", 3)
    left join {{ ref('dim_date') }} d
        on d.date = e.interview_date
),

base_screening as (
    -- One-time dimension mapping for SCREENING dataset (Unique_ID grain)
    select
        s."Unique_ID" as unique_id,
        coalesce(g.gender_key, 'unset') as gender_key,
        coalesce(ag.age_group_key, 'unset') as age_group_key,
        coalesce(ew.epi_week_key, 'unset') as epi_week_key,
        coalesce(f.facility_key, 'unset') as facility_key,
        coalesce(d.date_key, 'unset') as date_key,
        s."Eligible" as eligible_flag,
        s.interview_date
    from {{ ref('stg_afi_screening') }} s
    left join {{ ref('dim_gender') }} g
        on g.code = s."PatientGender"
    left join {{ ref('dim_age_group_afi_and_mortality') }} ag
        on s."Ageyears" >= ag.start_age and s."Ageyears" < ag.end_age
    left join {{ ref('dim_epi_week') }} ew
        on s.interview_date >= ew.start_of_week and s.interview_date <= ew.end_of_week
    left join {{ ref('dim_facility') }} f
        on f.afi_study_site_id = s."StudySite"
    left join {{ ref('dim_date') }} d
        on d.date = s.interview_date
),

screened_eligible as (
    select
        gender_key, age_group_key, epi_week_key, facility_key, date_key,
        count(distinct unique_id) as screened,
        sum(coalesce(eligible_flag, 0)) as eligible
    from base_screening
    group by 1,2,3,4,5
),

enrolled as (
    select
        gender_key, age_group_key, epi_week_key, facility_key, date_key,
        count(distinct pid) as enrolled
    from base_enroll
    group by 1,2,3,4,5
),

eligible_for_sampling as (
    select
        b.gender_key, b.age_group_key, b.epi_week_key, b.facility_key, b.date_key,
        count(distinct c."PID") as eligible_for_sampling
    from {{ ref('intermediate_afi_case_classification') }} c
    join base_enroll b
        on b.pid = c."PID"
    where c.CaseClassification in ('SARI', 'UF', 'MERS-CoV')
    group by 1,2,3,4,5
),

sampled as (
    select
        b.gender_key, b.age_group_key, b.epi_week_key, b.facility_key, b.date_key,
        count(distinct sc."PID") as sampled
    from {{ ref('stg_afi_sample_collection') }} sc
    join base_enroll b
        on b.pid = sc."PID"
    where sc."PID" is not null
      and sc."Barcode" not in (0, 22, 88888, 222222, 1111111, 11110000, 22220000, 22222222, 88888888)
    group by 1,2,3,4,5
),

-- UNION ALL then aggregate = faster than multiple FULL JOINs
stacked_all as (
    select gender_key, age_group_key, epi_week_key, facility_key, date_key,
           screened, eligible, 0::bigint as enrolled, 0::bigint as eligible_for_sampling, 0::bigint as sampled
    from screened_eligible

    union all
    select gender_key, age_group_key, epi_week_key, facility_key, date_key,
           0::bigint, 0::bigint, enrolled, 0::bigint, 0::bigint
    from enrolled

    union all
    select gender_key, age_group_key, epi_week_key, facility_key, date_key,
           0::bigint, 0::bigint, 0::bigint, eligible_for_sampling, 0::bigint
    from eligible_for_sampling

    union all
    select gender_key, age_group_key, epi_week_key, facility_key, date_key,
           0::bigint, 0::bigint, 0::bigint, 0::bigint, sampled
    from sampled
)

select
    gender_key,
    age_group_key,
    epi_week_key,
    facility_key,
    date_key,
    sum(screened) as screened,
    sum(eligible) as eligible,
    sum(enrolled) as enrolled,
    sum(eligible_for_sampling) as eligible_for_sampling,
    sum(sampled) as sampled,
    cast(current_date as date) as load_date
from stacked_all
group by 1,2,3,4,5

