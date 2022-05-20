from copy import deepcopy
import json
import pandas as pd

column_names = pd.read_csv("Column_names.csv")
cols = list(column_names.columns)
#cols="subjectid,informed_consent,missing_data,lifetime_ap_exposure_screen,psychosocial_treatment_form,past_pharmaceutical_treatment,current_pharmaceutical_treatment_floating_med_125,adverse_events,resource_use_log,sociodemographics,health_conditions_medical_historypsychiatric_histo,health_conditions_genetics_fluid_biomarkers,scid5_schizotypal_personality_sciddpq,scid5_psychosis_mood_substance_abuse,traumatic_brain_injury_screen,family_interview_for_genetic_studies_figs,sofas_screening,sofas_followup,psychs_p1p8,psychs_p9ac32,inclusionexclusion_criteria_review,premorbid_adjustment_scale,perceived_discrimination_scale,pubertal_developmental_scale,penncnb,iq_assessment_wasiii_wiscv_waisiv,premorbid_iq_reading_accuracy,eeg_run_sheet,mri_run_sheet,digital_biomarkers_mindlamp_onboarding,digital_biomarkers_mindlamp_checkin,digital_biomarkers_axivity_onboarding,digital_biomarkers_axivity_checkin,current_health_status,daily_activity_and_saliva_sample_collection,blood_sample_preanalytic_quality_assurance,cbc_with_differential,speech_sampling_run_sheet,nsipr,cdss,oasis,assist,cssrs_baseline,cssrs_followup,pss,global_functioning_social_scale,global_functioning_social_scale_followup,global_functioning_role_scale,global_functioning_role_scale_followup,item_promis_for_sleep,bprs,pgis,psychosis_polyrisk_score,recruitment_source,coenrollment_form,guid_form,ra_prediction,sips_adult_consent,current_pharmaceutical_treatment_floating_med_2650"

with open('formqc-1.json') as f:
    dict1= json.load(f)

template= dict1['config'][0]
print(template)

dict1['config']=[]
print(dict1)

for col in cols:
	# taking the first part of the variable name for a category
	category = col.partition('_')
	category = category[0]
	
	# using a different template depending on the end of the variable name
	if col.endswith('_complete'):
		template2= deepcopy(template)
		template2['variable']= col
		template2['label']=col
		template2['category']=category

	if col.endswith('_tot'):
		template2= deepcopy(template)
		template2['variable']= col
		template2['label']=col
		template2['category']=category

	if col.endswith('_perc'):
		template2= deepcopy(template)
		template2['variable']= col
		template2['label']=col
		template2['category']=category

	if col.endswith('_date'):
		template2= deepcopy(template)
		template2['variable']= col
		template2['label']=col
		template2['category']=category	

	if col.endswith('_miss'):
		template2= deepcopy(template)
		template2['variable']= col
		template2['label']=col
		template2['category']=category

	else:
		template2= deepcopy(template)
		template2['variable']= col
		template2['label']=col
		template2['category']=category	

	dict1['config'].append(template2)

dict1['name']='formqc-4'

with open('formqc-4.json','w') as f:
    json.dump(dict1,f,indent=3)



