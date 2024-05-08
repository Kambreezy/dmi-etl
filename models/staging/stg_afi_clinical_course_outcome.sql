select 
  "Unique_ID", 
  "ID", 
  "Source", 
  "PID", 
  "Interviewdate", 
  "Abstractiondate", 
  "Referred", 
  "OtherReferred", 
  "Crystapenstart", 
  "Crystapenstop", 
  "Penicilinstart", 
  "Penicilinstop", 
  "Chloramphenicolstart", 
  "Chloramphenicolstop", 
  "Trimethroprimsulfastart", 
  "Trimethroprimsulfastop", 
  "Nalidixicacidstart", 
  "Nalidixicacidstop", 
  "Ampicillinstart", 
  "Ampicillinstop", 
  "Amoxcluvanicstart", 
  "Amoxcluvanicstop", 
  "Gentamycinstart", 
  "Gentamycinstop", 
  "Ceftriaxonstart", 
  "Ceftriaxonstop", 
  "Otherantibiotic", 
  "Otherantibioticstart", 
  "Otherantibioticstop", 
  "Coartemstart", 
  "Coartemstop", 
  "Sulfadoxinestart", 
  "Sulfadoxinestop", 
  "Amodiaquinstart", 
  "Amodiaquinstop", 
  "Quininestart", 
  "Quininestop", 
  "Otherantimal", 
  "Otherantimalstart", 
  "Otherantimalstop", 
  "Patientadmitted", 
  "Patientadmitteddays", 
  "Fullhemogram", 
  "Hemoglobinlevel", 
  "Whitebloodcell", 
  "Neutrophil", 
  "Lymphocyt", 
  "Erythrocyte", 
  "CSFAppearance", 
  "CSFWhiteblood", 
  "CSFRedblood", 
  "UrineWhiteblood", 
  "UrineRedblood", 
  "Bacteria", 
  "Outcome", 
  "Dateoutcome", 
  "Causeofdeath", 
  "Causeofdeathother", 
  rec_status, 
  "Datecreated", 
  "Createdby", 
  "Dateupdated", 
  "Updatedby", 
  "MigratedAt", 
  "UUID", 
  otherantibioticspecify, 
  patientadhivstatus, 
  csfanalysis, 
  protein, 
  glucose, 
  urinemicroscopydone, 
  icuadmission, 
  icuadmdatedocumented, 
  icuadmissiondate, 
  icuadischarge, 
  icudischagedatedo, 
  icudischargedate_2, 
  suppoxygen, 
  suppoxygen_dates_doc, 
  suppoxystartdate, 
  suppoxystopdate, 
  mechanicalventilation, 
  mechventsdates_doc, 
  mechventstart, 
  mechventstop, 
  ventilationlength, 
  ards, 
  ards_startdate, 
  outcomestaff, 
  redcap_data_access_group, 
  ipd_no, 
  duplicate_pid, 
  antibioticsgiven____1000, 
  antimalarial_2____1000, 
  discharge_diagnosis___96, 
  discharge_diagnosis___999, 
  discharge_diagnosis____8, 
  discharge_diagnosis____1000, 
  part_six_clinical_course_and_outcome_for_netbook_p_complete 
from {{ source('central_raw_afi', 'clinical_course_outcome') }}
