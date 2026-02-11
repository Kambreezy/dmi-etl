{{
    config(
        materialized='table',
        post_hook= ["create index if not exists idx_{{ this.identifier }}__screening_date on {{ this }} (screening_date)",
      "create index if not exists idx_{{ this.identifier }}__site_date on {{ this }} (site, screening_date)",
      "create index if not exists idx_{{ this.identifier }}__unique_id on {{ this }} (\"Unique_ID\")",
      "create index if not exists idx_{{ this.identifier }}__case on {{ this }} (proposed_combined_case)
    "])
}}


SELECT "Unique_ID",
  "source",
       pid,
        screening_interviewdate::DATE as screening_date ,
       ROUND(screeningpoint:: NUMERIC) :: INT as screeningpoint,

CASE
  WHEN eligible::TEXT IN ('0') THEN 0
  WHEN eligible::TEXT IN ('1', 'Yes', 'yes') THEN 1
  WHEN eligible::TEXT IN ('2', 'No', 'no') THEN 2
  ELSE NULL
END AS eligible,
    
         ROUND(gender:: numeric)::INT as gender,
            case
	when consent = '<NA>' then 99
	-- Replace '<NA>' with 99
	else consent::INT
end as consent,
          non_enr_reason,
           ROUND(exactfeverdays::numeric) :: INT as exactfeverdays,
               enr_interviewdate::DATE as enr_interviewdate,
                 high_temp_recorded::DECIMAL as high_temp_recorded,
-- Convert to DECIMAL
diagnosis,
       discharge_diagnosis,
       causeofdeathother,
         outcome::INT as outcome,
-- Convert to INT
proposed_combined_case,
              sampled::INT as sampled,
              swabsamplebarcodes,
               swabspecimencolldate::DATE as swabspecimencolldate,
               sars_datereceived::DATE as sars_datereceived,
                sars_datetested::DATE as sars_datetested,
                  swab_tat_receiving::DECIMAL as swab_tat_receiving,
                        swab_tat_testing::DECIMAL as swab_tat_testing,
                    fluresult,
       rsvresult,
       sc2result,
       wholebloodbarcode,
       tac_tat_receiving::DECIMAL as tac_tat_receiving,
      tac_tat_testing::DECIMAL as tac_tat_testing,
      "Target",
             "TacResult",
       "PCR_MalariaSpecies",
       "TACmalariaResult",
       malariasmear_barcode,
       smear_tat_receiving::DECIMAL as smear_tat_receiving,
       read1_tat_testing::DECIMAL as read1_tat_testing,
       read2_tat_testing::DECIMAL as read2_tat_testing,
       read3_tat_testing::DECIMAL as read3_tat_testing,
       read1_result::DECIMAL as read1_result,
       read2_result::DECIMAL as read2_result,
         "Read_1_2_outcome",
       con_dis_check,
       "Final_Result",
       malariardt_barcode,
        malariardtres::INTEGER as malariardtres,
       malariares::INTEGER as malariares,
       serum_barcode,
       site ::INTEGER as site,
        ROUND(calculated_age_days:: numeric)::INT as calculated_age_days,
       diagnosisother
FROM   {{source('central_raw_afi', 'afi_surveillance_table')}}


