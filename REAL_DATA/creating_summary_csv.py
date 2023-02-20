import sys
import numpy as np
import pandas as pd
import argparse
import json
import os
from datetime import datetime
from datetime import date

def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d") 
    return abs((d2 - d1).days)

## getting today's date
today = date.today()
today = today.strftime("%Y-%m-%d")

input = "/data/predict1/data_from_nda/formqc/"
output = "/data/predict1/data_from_nda/AMP-SCZ_summary_csv/"

# prescient data
screening_prescient = pd.read_csv(input + "combined-PRESCIENT-form_screening-day1to1.csv")
baseline_prescient = pd.read_csv(input + "combined-PRESCIENT-form_baseline-day1to1.csv")
month1_prescient = pd.read_csv(input + "combined-PRESCIENT-form_month1-day1to1.csv")
month2_prescient = pd.read_csv(input + "combined-PRESCIENT-form_month2-day1to1.csv")

#print(screening_prescient.T)


screening_prescient_1 = screening_prescient[['subjectid', 'site', 'chrguid_guid', 'chric_consent_date', 'chrcrit_part', 'chrcrit_included', 'visit_status_string']]
#print(screening_prescient)

## screening complet - "['chrcrit_missing'] not in index"
screening_prescient_complete = screening_prescient[['subjectid', 'chrrecruit_missing', 'recruitment_source', 'chrap_missing', 'lifetime_ap_exposure_screen','chrpharm_med_past', 'past_pharmaceutical_treatment','chrhealth_missing', 'health_conditions_genetics_fluid_biomarkers', 'inclusionexclusion_criteria_review', 'chrschizotypal_missing', 'scid5_schizotypal_personality_sciddpq', 'chrtbi_missing', 'traumatic_brain_injury_screen','chrfigs_mother_info', 'family_interview_for_genetic_studies_figs', 'chrsofas_missing', 'sofas_screening','chrpsychs_scr_ac1', 'psychs_p1p8', 'psychs_p9ac32']]

## baseline complete - KeyError: "['chrpds_missing', 'chrmri_missing', 'chrblood_plasma_freeze', 'chrblood_serum_freeze', 'chreeg_missing', 'chrassist_missing', 'chrcdss_missing', 'chrblood_wholeblood_freeze', 'chrblood_buffy_freeze', 'chrspeech_missing', 'chroasis_missing', 'chrbprs_missing', 'chrpgi_missing'] not in index"
baseline_prescient_complete = baseline_prescient[['subjectid','interview_age', 'chrdemo_sexassigned', 'sociodemographics','chrscid_remote', 'scid5_psychosis_mood_substance_abuse', 'chrsofas_missing_fu', 'sofas_followup', 'chrpsychs_missing_spec_fu','psychs_p1p8_fu','chrpsychs_fu_ac9','chrpsychs_fu_ac15','chrpsychs_fu_ac21','chrpsychs_fu_ac27',  'psychs_p9ac32_fu', 'chrpenn_missing', 'chrpenn_complete', 'penncnb','chriq_fsiq', 'iq_assessment_wasiii_wiscv_waisiv', 'chrpreiq_missing', 'premorbid_iq_reading_accuracy','chreeg_zip', 'eeg_run_sheet', 'chrmri_localizerseq', 'chrmri_aahscout', 'chrmri_localizeraligned', 'chrmri_dmap', 'chrmri_dmpa', 'chrmri_t1', 'chrmri_t2', 'chrmri_dmap2', 'chrmri_dmpa2', 'chrmri_rfmriap', 'chrmri_rfmripa', 'chrmri_dmri_b0', 'chrmri_dmri176', 'chrmri_dmri_b0_2', 'chrmri_dmap3', 'chrmri_dmpa3', 'chrmri_rfmriap2', 'chrmri_rfmripa2', 'mri_run_sheet','chrdbb_opt_in', 'digital_biomarkers_mindlamp_onboarding','chrax_device_given', 'digital_biomarkers_axivity_onboarding', 'chrchs_missing', 'current_health_status', 'chrsaliva_missing', 'daily_activity_and_saliva_sample_collection', 'chrsaliva_ice', 'chrblood_missing', 'blood_sample_preanalytic_quality_assurance', 'chrcbc_missing', 'cbc_with_differential', 'chrcbc_wbcsum', 'chrspeech_upload', 'speech_sampling_run_sheet', 'chrnsipr_missing', 'nsipr', 'cdss',  'assist','chrcssrsb_si1l', 'cssrs_baseline','chrgfs_gf_social_scale', 'global_functioning_social_scale','chrgfr_gf_role_scole', 'global_functioning_role_scale','chrdim_dim_yesno_q1_1', 'perceived_discrimination_scale', 'pubertal_developmental_scale', 'oasis', 'chrpromis_missing', 'item_promis_for_sleep', 'chrpss_missing', 'perceived_stress_scale','chrpps_writing', 'psychosis_polyrisk_score',  'pgis', 'bprs','chrpred_transition', 'ra_prediction']]

## month1 - "['chrdemo_missing'] not in index", KeyError: "['chrpromis_missing', 'chrpgi_missing', 'chrcdss_missing', 'chroasis_missing', 'chrbprs_missing'] not in index"
month1_prescient_complete = month1_prescient[['subjectid', 'chrsofas_missing_fu', 'sofas_followup','chrpsychs_fu_ac9','chrpsychs_fu_ac15','chrpsychs_fu_ac21','chrpsychs_fu_ac27', 'psychs_p1p8_fu', 'psychs_p9ac32_fu', 'chrpas_missing', 'premorbid_adjustment_scale','chrdig_monthly_percent', 'digital_biomarkers_mindlamp_checkin','chraxci_device_return', 'digital_biomarkers_axivity_checkin', 'chrnsipr_missing', 'nsipr', 'cdss',  'chrgfssfu_missing', 'global_functioning_social_scale_followup', 'chrgfrsfu_missing', 'global_functioning_role_scale_followup',  'oasis', 'chrpss_missing', 'perceived_stress_scale', 'pgis',  'bprs']]

## month 2 prescient
#KeyError: "['chrassist_missing', 'chrpenn', 'chrblood_wholeblood_freeze', 'chrpgi_missing', 'chreeg_missing', 'chrblood_buffy_freeze', 'chrcdss_missing', 'chrspeech_missing', 'chroasis_missing', 'chrbprs_missing', 'chrblood_plasma_freeze', 'chrblood_serum_freeze', 'chrmri_missing'] not in index"
month2_prescient_complete = month2_prescient[['subjectid', 'chrsofas_missing_fu', 'sofas_followup','chrpsychs_fu_ac9','chrpsychs_fu_ac15','chrpsychs_fu_ac21','chrpsychs_fu_ac27', 'psychs_p1p8_fu', 'psychs_p9ac32_fu', 'chrpenn_missing', 'penncnb', 'chreeg_zip', 'eeg_run_sheet', 'chrmri_localizerseq', 'chrmri_aahscout', 'chrmri_localizeraligned', 'chrmri_dmap', 'chrmri_dmpa', 'chrmri_t1', 'chrmri_t2', 'chrmri_dmap2', 'chrmri_dmpa2', 'chrmri_rfmriap', 'chrmri_rfmripa', 'chrmri_dmri_b0', 'chrmri_dmri176', 'chrmri_dmri_b0_2', 'chrmri_dmap3', 'chrmri_dmpa3', 'chrmri_rfmriap2', 'chrmri_rfmripa2', 'mri_run_sheet', 'digital_biomarkers_mindlamp_checkin','chrdig_monthly_percent','chraxci_device_return', 'digital_biomarkers_axivity_checkin', 'chrchs_missing', 'current_health_status', 'chrsaliva_missing', 'daily_activity_and_saliva_sample_collection', 'chrsaliva_ice', 'chrblood_missing', 'blood_sample_preanalytic_quality_assurance',  'chrcbc_missing', 'cbc_with_differential', 'chrcbc_wbcsum', 'chrspeech_upload', 'speech_sampling_run_sheet', 'chrnsipr_missing', 'nsipr', 'cdss', 'assist', 'chrcssrsfu_missing', 'cssrs_followup', 'chrgfssfu_missing', 'global_functioning_social_scale_followup', 'chrgfrsfu_missing', 'global_functioning_role_scale_followup', 'oasis', 'chrpromis_missing', 'item_promis_for_sleep', 'chrpss_missing', 'perceived_stress_scale', 'pgis', 'bprs']]

## screening complete
screening_prescient_complete = pd.merge(screening_prescient_1, screening_prescient_complete, on=['subjectid'])
screening_prescient_complete['Network'] = "prescient"
screening_prescient_complete.to_csv(output + "Screening_prescient_complete_forms_" + today + ".csv", sep=',', index = False, header=True)

## Baseline 
baseline_prescient_complete = pd.merge(screening_prescient_1, baseline_prescient_complete, on=['subjectid'])
baseline_prescient_complete['Network'] = "prescient"
baseline_prescient_complete.to_csv(output + "Baseline_prescient_complete_forms_" + today + ".csv", sep=',', index = False, header=True)

## Month 1
month1_prescient_complete = pd.merge(screening_prescient_1, month1_prescient_complete, on=['subjectid'])
month1_prescient_complete['Network'] = "prescient"
month1_prescient_complete.to_csv(output + "Month1_prescient_complete_forms_" + today + ".csv", sep=',', index = False, header=True)

## Month 2
print("Creating month 2 csv")
month2_prescient_complete = pd.merge(screening_prescient_1, month2_prescient_complete, on=['subjectid'])
month2_prescient_complete['Network'] = "prescient"
month2_prescient_complete.to_csv(output + "Month2_prescient_complete_forms_" + today + ".csv", sep=',', index = False, header=True)



baseline_prescient_missing = baseline_prescient[['subjectid', 'interview_age', 'chrdemo_sexassigned', 'chrdemo_hispanic_latino', 'chrdemo_racial_back___1', 'chrdemo_racial_back___2', 'chrdemo_racial_back___3', 'chrdemo_racial_back___4', 'chrdemo_racial_back___5', 'chrdemo_racial_back___6', 'chrdemo_racial_back___7', 'chrdemo_racial_back___8','no_missing_pdiscrims', 'no_missing_pubds', 'no_missing_nsipr', 'no_missing_cdss', 'no_missing_oasis', 'no_missing_pss', 'no_missing_promis', 'no_missing_bprs', 'no_missing_cdssrsb', 'no_missing_sofas', 'no_missing_glob_func_role', 'no_missing_glob_func_social', 'no_missing_penncnb', 'no_missing_wrat', 'no_missing_wasiiq']]

baseline_prescient = baseline_prescient[['subjectid', 'interview_age', 'chrdemo_sexassigned', 'chrdemo_education', 'chrdemo_parent_edu_first', 'chrdemo_parent_edu_other',  'chrdemo_hispanic_latino', 'chrdemo_racial_back___1', 'chrdemo_racial_back___2', 'chrdemo_racial_back___3', 'chrdemo_racial_back___4', 'chrdemo_racial_back___5', 'chrdemo_racial_back___6', 'chrdemo_racial_back___7', 'chrdemo_racial_back___8']] 
print(baseline_prescient)

prescient = pd.merge(screening_prescient_1, baseline_prescient, on=['subjectid'])
prescient['Network'] = "Prescient"
#print(prescient)
prescient_missing = pd.merge(screening_prescient_1, baseline_prescient_missing, on=['subjectid'])
prescient_missing['Network'] = "Prescient"

############## pronet data
screening_pronet = pd.read_csv(input + "combined-PRONET-form_screening-day1to1.csv")
baseline_pronet = pd.read_csv(input + "combined-PRONET-form_baseline-day1to1.csv")
month1_pronet = pd.read_csv(input + "combined-PRONET-form_month1-day1to1.csv")
month2_pronet = pd.read_csv(input + "combined-PRONET-form_month2-day1to1.csv")


screening_pronet_1 = screening_pronet[['subjectid', 'site', 'chrguid_guid', 'chric_consent_date', 'chrcrit_part', 'chrcrit_included', 'visit_status_string']]

## excluded adverse events, psychosocial treatment, resource use log
screening_pronet_complete = screening_pronet[['subjectid', 'chrrecruit_missing', 'recruitment_source_complete', 'chrap_missing', 'lifetime_ap_exposure_screen_complete','chrpharm_med_past', 'past_pharmaceutical_treatment_complete','chrhealth_missing', 'health_conditions_genetics_fluid_biomarkers_complete', 'chrcrit_missing', 'inclusionexclusion_criteria_review_complete', 'chrschizotypal_missing', 'scid5_schizotypal_personality_sciddpq_complete', 'chrtbi_missing', 'traumatic_brain_injury_screen_complete','chrfigs_mother_info', 'family_interview_for_genetic_studies_figs_complete', 'chrsofas_missing', 'sofas_screening_complete','chrpsychs_scr_ac1', 'psychs_p1p8_complete', 'psychs_p9ac32_complete']]

# "['chrdemo_missing', 'chrcssrsb_missing', 'chrpred_missing', 'chrgfrs_missing', 'chrpps_missing', 'chriq_missing', 'chrdig_missing', 'chrgfss_missing', 'chraxci_qaqc1', 'chrdim_missing'] not in index"
baseline_pronet_complete = baseline_pronet[['subjectid','interview_age', 'chrdemo_sexassigned', 'sociodemographics_complete','chrscid_remote', 'scid5_psychosis_mood_substance_abuse_complete', 'chrsofas_missing_fu', 'sofas_followup_complete','chrpsychs_missing_spec_fu','psychs_p1p8_fu_complete','chrpsychs_fu_ac9','chrpsychs_fu_ac15','chrpsychs_fu_ac21','chrpsychs_fu_ac27',  'psychs_p9ac32_fu_complete', 'chrpenn_missing', 'chrpenn_complete', 'penncnb_complete','chriq_fsiq', 'iq_assessment_wasiii_wiscv_waisiv_complete', 'chrpreiq_missing', 'premorbid_iq_reading_accuracy_complete', 'chreeg_missing','chreeg_zip', 'eeg_run_sheet_complete', 'chrmri_localizerseq', 'chrmri_aahscout', 'chrmri_localizeraligned', 'chrmri_dmap', 'chrmri_dmpa', 'chrmri_t1', 'chrmri_t2', 'chrmri_dmap2', 'chrmri_dmpa2', 'chrmri_rfmriap', 'chrmri_rfmripa', 'chrmri_dmri_b0', 'chrmri_dmri176', 'chrmri_dmri_b0_2', 'chrmri_dmap3', 'chrmri_dmpa3', 'chrmri_rfmriap2', 'chrmri_rfmripa2', 'chrmri_missing', 'mri_run_sheet_complete','chrdbb_opt_in', 'digital_biomarkers_mindlamp_onboarding_complete','chrax_device_given', 'digital_biomarkers_axivity_onboarding_complete', 'chrchs_missing', 'current_health_status_complete', 'chrsaliva_missing', 'daily_activity_and_saliva_sample_collection_complete', 'chrsaliva_ice', 'chrblood_missing', 'blood_sample_preanalytic_quality_assurance_complete', 'chrblood_wholeblood_freeze', 'chrblood_serum_freeze', 'chrblood_buffy_freeze', 'chrblood_plasma_freeze', 'chrcbc_missing', 'cbc_with_differential_complete', 'chrcbc_wbcsum', 'chrspeech_upload', 'chrspeech_missing', 'speech_sampling_run_sheet_complete', 'chrnsipr_missing', 'nsipr_complete', 'chrcdss_missing', 'cdss_complete', 'chrassist_missing', 'assist_complete','chrcssrsb_si1l', 'cssrs_baseline_complete','chrgfs_gf_social_scale', 'global_functioning_social_scale_complete','chrgfr_gf_role_scole', 'global_functioning_role_scale_complete','chrdim_dim_yesno_q1_1', 'perceived_discrimination_scale_complete', 'chrpds_missing', 'pubertal_developmental_scale_complete', 'chroasis_missing', 'oasis_complete', 'chrpromis_missing', 'item_promis_for_sleep_complete', 'chrpss_missing', 'perceived_stress_scale_complete','chrpps_writing', 'psychosis_polyrisk_score_complete', 'chrpgi_missing', 'pgis_complete', 'chrbprs_missing', 'bprs_complete','chrpred_transition', 'ra_prediction_complete']]

## month1 - "['chrdemo_missing'] not in index"
month1_pronet_complete = month1_pronet[['subjectid', 'chrsofas_missing_fu', 'sofas_followup_complete','chrpsychs_fu_ac9','chrpsychs_fu_ac15','chrpsychs_fu_ac21','chrpsychs_fu_ac27', 'psychs_p1p8_fu_complete', 'psychs_p9ac32_fu_complete', 'chrpas_missing', 'premorbid_adjustment_scale_complete','chrdig_monthly_percent','digital_biomarkers_mindlamp_checkin_complete','chraxci_device_return', 'digital_biomarkers_axivity_checkin_complete', 'chrnsipr_missing', 'nsipr_complete', 'chrcdss_missing', 'cdss_complete',  'chrgfssfu_missing', 'global_functioning_social_scale_followup_complete', 'chrgfrsfu_missing', 'global_functioning_role_scale_followup_complete', 'chroasis_missing', 'oasis_complete', 'chrpromis_missing', 'chrpss_missing', 'perceived_stress_scale_complete', 'chrpgi_missing', 'pgis_complete', 'chrbprs_missing', 'bprs_complete']]

## month 2 "['chrdig_missing', 'chraxci_qaqc1'] not in index"
month2_pronet_complete = month2_pronet[['subjectid', 'chrsofas_missing_fu', 'sofas_followup_complete','chrpsychs_fu_ac9','chrpsychs_fu_ac15','chrpsychs_fu_ac21','chrpsychs_fu_ac27', 'psychs_p1p8_fu_complete', 'psychs_p9ac32_fu_complete', 'chrpenn_missing', 'chrpenn_complete', 'penncnb_complete', 'chreeg_missing','chreeg_zip', 'eeg_run_sheet_complete', 'chrmri_localizerseq', 'chrmri_aahscout', 'chrmri_localizeraligned', 'chrmri_dmap', 'chrmri_dmpa', 'chrmri_t1', 'chrmri_t2', 'chrmri_dmap2', 'chrmri_dmpa2', 'chrmri_rfmriap', 'chrmri_rfmripa', 'chrmri_dmri_b0', 'chrmri_dmri176', 'chrmri_dmri_b0_2', 'chrmri_dmap3', 'chrmri_dmpa3', 'chrmri_rfmriap2', 'chrmri_rfmripa2', 'chrmri_missing', 'mri_run_sheet_complete','chrdig_monthly_percent', 'digital_biomarkers_mindlamp_checkin_complete','chraxci_device_return', 'digital_biomarkers_axivity_checkin_complete', 'chrchs_missing', 'current_health_status_complete', 'chrsaliva_missing', 'daily_activity_and_saliva_sample_collection_complete', 'chrsaliva_ice', 'chrblood_missing', 'blood_sample_preanalytic_quality_assurance_complete', 'chrblood_wholeblood_freeze', 'chrblood_serum_freeze', 'chrblood_buffy_freeze', 'chrblood_plasma_freeze', 'chrcbc_missing', 'cbc_with_differential_complete', 'chrcbc_wbcsum', 'chrspeech_upload', 'chrspeech_missing', 'speech_sampling_run_sheet_complete', 'chrnsipr_missing', 'nsipr_complete', 'chrcdss_missing', 'cdss_complete', 'chrassist_missing', 'assist_complete', 'chrcssrsfu_missing', 'cssrs_followup_complete', 'chrgfssfu_missing', 'global_functioning_social_scale_followup_complete', 'chrgfrsfu_missing', 'global_functioning_role_scale_followup_complete', 'chroasis_missing', 'oasis_complete', 'chrpromis_missing', 'item_promis_for_sleep_complete', 'chrpss_missing', 'perceived_stress_scale_complete', 'chrpgi_missing', 'pgis_complete', 'chrbprs_missing', 'bprs_complete']]

baseline_pronet_missing = baseline_pronet[['subjectid','interview_age', 'chrdemo_sexassigned', 'chrdemo_hispanic_latino', 'chrdemo_racial_back___1', 'chrdemo_racial_back___2', 'chrdemo_racial_back___3', 'chrdemo_racial_back___4', 'chrdemo_racial_back___5', 'chrdemo_racial_back___6', 'chrdemo_racial_back___7', 'chrdemo_racial_back___8','no_missing_pdiscrims', 'no_missing_pubds', 'no_missing_nsipr', 'no_missing_cdss', 'no_missing_oasis', 'no_missing_pss', 'no_missing_promis', 'no_missing_bprs', 'no_missing_cdssrsb', 'no_missing_sofas', 'no_missing_glob_func_role', 'no_missing_glob_func_social', 'no_missing_penncnb', 'no_missing_wrat', 'no_missing_wasiiq']]

baseline_pronet_1 = baseline_pronet[['subjectid', 'interview_age', 'chrdemo_sexassigned','chrdemo_education', 'chrdemo_parent_edu_first', 'chrdemo_parent_edu_other' , 'chrdemo_hispanic_latino', 'chrdemo_racial_back___1', 'chrdemo_racial_back___2', 'chrdemo_racial_back___3', 'chrdemo_racial_back___4', 'chrdemo_racial_back___5', 'chrdemo_racial_back___6', 'chrdemo_racial_back___7', 'chrdemo_racial_back___8']]

#print(baseline_pronet_missing)

#### Summary of complete and missing for pronet
## Screening 
screening_pronet_complete = pd.merge(screening_pronet_1, screening_pronet_complete, on=['subjectid'])
screening_pronet_complete['Network'] = "Pronet"
screening_pronet_complete.to_csv(output + "Screening_Pronet_complete_forms_" + today + ".csv", sep=',', index = False, header=True)

## Baseline 
baseline_pronet_complete = pd.merge(screening_pronet_1, baseline_pronet_complete, on=['subjectid'])
baseline_pronet_complete['Network'] = "Pronet"
baseline_pronet_complete.to_csv(output + "Baseline_Pronet_complete_forms_" + today + ".csv", sep=',', index = False, header=True)

## Month 1
month1_pronet_complete = pd.merge(screening_pronet_1, month1_pronet_complete, on=['subjectid'])
month1_pronet_complete['Network'] = "Pronet"
month1_pronet_complete.to_csv(output + "Month1_Pronet_complete_forms_" + today + ".csv", sep=',', index = False, header=True)

## Month 2
month2_pronet_complete = pd.merge(screening_pronet_1, month2_pronet_complete, on=['subjectid'])
month2_pronet_complete['Network'] = "Pronet"
month2_pronet_complete.to_csv(output + "Month2_Pronet_complete_forms_" + today + ".csv", sep=',', index = False, header=True)

# missing data
pronet_missing = pd.merge(screening_pronet, baseline_pronet_missing, on=['subjectid'])
pronet_missing['Network'] = "Pronet"

missing = pd.concat([pronet_missing, prescient_missing],axis=0, ignore_index=True)
missing.to_csv(output + "Summary_missing_forms_" + today + ".csv", sep=',', index = False, header=True)
#print(pronet_missing)



######################## Summary csv
pronet = pd.merge(screening_pronet_1, baseline_pronet_1, on=['subjectid'])
pronet['Network'] = "Pronet"

ampscz = pd.concat([pronet, prescient],axis=0, ignore_index=True)

ampscz = ampscz.rename(columns={'chrdemo_education':'Years_Education', 'chrdemo_parent_edu_first':'Parent1_Education', 'chrdemo_parent_edu_other':'Parent2_Education', 'chrdemo_racial_back___1': 'Indigenous', 'chrdemo_racial_back___2': 'East Asian', 'chrdemo_racial_back___3': 'Southeast Asian', 'chrdemo_racial_back___4': 'South Asian', 'chrdemo_racial_back___5': 'Black/African American', 'chrdemo_racial_back___6': 'West/Central Asian & Middle Eastern', 'chrdemo_racial_back___7': 'White/European/North American/Australian', 'chrdemo_racial_back___8': 'Native Hawaiian or Native Pacific islander',  'chrdemo_hispanic_latino': 'Hispanic_Latino',  'chrguid_guid': 'GUID', 'chrcrit_part': 'Group', 'chrcrit_included': 'Included', 'visit_status_string': 'Current_visit', 'interview_age': 'Age', 'chrdemo_sexassigned': 'Sex_at_birth'})

ampscz['Included'] = ampscz['Included'].map({1:'yes' , 0:'no'})
ampscz['Sex_at_birth'] = ampscz['Sex_at_birth'].map({1:'male' , 2:'female'})  
ampscz['Group'] = ampscz['Group'].map({1:'CHR' , 2:'HC'})  
ampscz['Hispanic_Latino'] = ampscz['Hispanic_Latino'].map({0:'no' , 1:'yes'})
#ampscz['Race'] = ampscz['Race'].map({1:'Indigenous' , 2:'East Asian', 3:'Southeast Asian', 4:'South Asian', 5:'Black/African American', 6:'West/Central Asian & Middle Eastern', 7:'White/European/North American/Australian', 8:'Native Hawaiian or Native Pacific islander'})
 
#ampscz.Included.map(dict(1=yes, 0=no))
#ampscz.Sex_at_birth.map(dict(1=male, 2=female))
#ampscz.Group.map(dict(1=CHR, 2=HC))

print("Final csv:")
print(ampscz)
print(ampscz.T)

# saving summary csv
ampscz.to_csv(output + "AMP-SCZ_participants_summary_" + today + ".csv", sep=',', index = False, header=True)
