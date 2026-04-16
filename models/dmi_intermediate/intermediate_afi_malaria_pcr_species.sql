with malaria_pcr_species_cte as (

    SELECT 
    malaria_pcr_species.pid,
    malaria_pcr_species.consent,
       CASE 
        WHEN "PCR_MalariaSpecies"  = 'Plasmodium spp.'THEN 3 
        WHEN "PCR_MalariaSpecies" = 'P. falciparum and P. vivax' THEN 1
        WHEN "PCR_MalariaSpecies" = 'Non-falciparum, non-vivax Plasmodium species' THEN 2
        WHEN "PCR_MalariaSpecies" = 'P. falciparum' THEN 4
        WHEN "PCR_MalariaSpecies" = 'P. vivax' THEN 5
        ELSE NULL
    END AS pcr_malaria_species_code,
    CASE 
        WHEN "PCR_MalariaSpecies"  = 'Plasmodium spp.'THEN 'Plasmodium unspeciated' 
        WHEN "PCR_MalariaSpecies" = 'P. falciparum and P. vivax' THEN 'P. falciparum and P. vivax'
        WHEN "PCR_MalariaSpecies" = 'Non-falciparum, non-vivax Plasmodium species' THEN 'Non-falciparum, non-vivax Plasmodium species'
        WHEN "PCR_MalariaSpecies" = 'P. falciparum' THEN 'P. falciparum'
        WHEN "PCR_MalariaSpecies" = 'P. vivax' THEN 'P. vivax'
        ELSE NULL
    END AS pcr_malaria_species

 from {{ ref('stg_afi_surveillance') }}  as malaria_pcr_species
) 
SELECT 
    pid,
    consent,
    pcr_malaria_species_code,
    pcr_malaria_species    
FROM malaria_pcr_species_cte