from copy import deepcopy
import json
import pandas as pd

#column_names = pd.read_csv("Column_names.csv")
#cols = list(column_names.columns)
name = "mri_run_sheet"
cols = ["subjectid" ,"Tot_variables", "Existing_variables", "Missing_spec" ,"Percentage"]
#cols="subjectid,informed_consent,missing_data,lifetime_ap_exposure_screen,psychosocial_treatment_form,past_pharmaceutical_treatment,current_pharmaceutical_treatment_floating_med_125,adverse_events,resource_use_log,sociodemographics,health_conditions_medical_historypsychiatric_histo,health_conditions_genetics_fluid_biomarkers,scid5_schizotypal_personality_sciddpq,scid5_psychosis_mood_substance_abuse,traumatic_brain_injury_screen,family_interview_for_genetic_studies_figs,sofas_screening,sofas_followup,psychs_p1p8,psychs_p9ac32,inclusionexclusion_criteria_review,premorbid_adjustment_scale,perceived_discrimination_scale,pubertal_developmental_scale,penncnb,iq_assessment_wasiii_wiscv_waisiv,premorbid_iq_reading_accuracy,eeg_run_sheet,mri_run_sheet,digital_biomarkers_mindlamp_onboarding,digital_biomarkers_mindlamp_checkin,digital_biomarkers_axivity_onboarding,digital_biomarkers_axivity_checkin,current_health_status,daily_activity_and_saliva_sample_collection,blood_sample_preanalytic_quality_assurance,cbc_with_differential,speech_sampling_run_sheet,nsipr,cdss,oasis,assist,cssrs_baseline,cssrs_followup,pss,global_functioning_social_scale,global_functioning_social_scale_followup,global_functioning_role_scale,global_functioning_role_scale_followup,item_promis_for_sleep,bprs,pgis,psychosis_polyrisk_score,recruitment_source,coenrollment_form,guid_form,ra_prediction,sips_adult_consent,current_pharmaceutical_treatment_floating_med_2650"

with open('formqc-percent.json') as f:
    dict1= json.load(f)
template_percent= dict1['config'][0]

with open('formqc-complete.json') as f:
    dict1= json.load(f)
template_complete= dict1['config'][0]

with open('formqc-date.json') as f:
    dict1= json.load(f)
template_date= dict1['config'][0]

with open('formqc-total.json') as f:
    dict1= json.load(f)
template_total= dict1['config'][0]

dict1['config']=[]
print(dict1)
print(template_percent)
print(template_date)
print(template_total)
print(template_complete)

for col in cols:
	# taking the first part of the variable name for a category
	#category = col.partition('_')
	#category = category[0]
	print(col)
	
	# using a different template depending on the end of the variable name
	if 'Existing_variables' in col:
		template2= deepcopy(template_total)
		template2['analysis']= name
		template2['variable']= col
		lab = " - Total Variables"
		template2['label']= name + lab
		template2['category']="forms_qc"
		dict1['config'].append(template2)

	if 'Percentage' in col:
		template2= deepcopy(template_percent)
		template2['analysis']= name
		template2['variable']= col
		template2['label']="Percentage Complete"
		template2['category']="forms_qc"
		dict1['config'].append(template2)

	if 'Missing_spec' in col:
		template2= deepcopy(template_date)
		template2['analysis']= name
		template2['variable']= col
		template2['label']="Missing specification"
		template2['category']="forms_qc"
		dict1['config'].append(template2)

	if 'subjectid' in col:
		template2= deepcopy(template_total)
		template2['analysis']= name
		template2['variable']= col
		template2['label']="Subject ID"
		template2['category']="id"
		dict1['config'].append(template2)


dict1['name']='mri_run_sheet_qc-1'

with open('mri_run_sheet_qc-1.json','w') as f:
    json.dump(dict1,f,indent=3)



