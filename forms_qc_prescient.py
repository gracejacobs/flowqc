import sys
import numpy as np
import pandas as pd
import argparse
import json
from datetime import datetime

def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

### Requires one arguements - ID number (YA00009) 
# ME57953

id = str(sys.argv[1])
print("ID: ", id)

#site=str(sys.argv[2])
site = id[0:2]
print("Site: ", site)

input_path = "/data/predict/data_from_nda_dev/Prescient/PHOENIX/PROTECTED/"
print("Input path: " + input_path)

output_path = "/data/predict/kcho/flow_test/formqc/"
print("Output path: " + output_path)

# there are double coenrollment, and health conditions, promis, lifetime ap, NSI/nsipr, premorbid iq, perceived stress scale, recruitment source, speech, MRI run sheet
# two inclusion criterias but they're a little different
# variety of ways empty cells are indicated

# TBI screen, FIGS, psychs p9ac32, scid5 psychosis mood substance use,IQ WASI, Blood sample, cbc with differential, assist, 
#ME57953.Prescient.Run_sheet_PennCNB_1.csv
#health_conditions_medical_historypsychiatric_histo

#


adverse = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_adverse_events.csv")
assist = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_assist.csv")
bprs = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_bprs.csv")
cdss = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_cdss.csv")
coenrollment = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_coenrollment_form.csv")
current_health = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_current_health_status.csv")
daily_activity = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_daily_activity_and_saliva_sample_collection.csv")
dig_axivity = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_digital_biomarkers_axivity_onboarding.csv")
dig_mindlamp = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_digital_biomarkers_mindlamp_onboarding.csv")
eeg = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_eeg_run_sheet.csv")
entry_status = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_entry_status.csv")
global_role = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_global_functioning_role_scale.csv")
global_social = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_global_functioning_social_scale.csv")
guid = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_guid_form.csv")
health_biomarkers = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_health_conditions_genetics_fluid_biomarkers.csv")
health_history = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_health_conditions_medical_historypsychiatric_histo.csv")
inclusion = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_inclusionexclusion_criteria_review.csv")
promis = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_item_promis_for_sleep.csv")
lifetime = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_lifetime_ap_exposure_screen.csv")
missing = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_missing_data.csv")
mri = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_mri_run_sheet.csv")
nsipr = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_nsipr.csv")
#pas = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_PAS.csv")
#pds = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_PDS.csv")
oasis = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_oasis.csv")
penn = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_penncnb.csv")
per_dis = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_perceived_discrimination_scale.csv")
per_stress = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_perceived_stress_scale.csv")
pgis = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_pgis.csv")
premorbidadjust = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_premorbid_adjustment_scale.csv")
premorbidiq = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_premorbid_iq_reading_accuracy.csv")
#penn_run_sheet = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"Prescient.Run_sheet_PennCNB_1.csv")
psychosis = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_psychosis_polyrisk_score.csv")
psychosocial = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_psychosocial_treatment_form.csv")
psychsp1p8 = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_psychs_p1p8.csv")
puberty = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_pubertal_developmental_scale.csv")
ra_pred = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_ra_prediction.csv")
recruitment = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_recruitment_source.csv")
# scid5_psychosis_mood_substance_abuse
scid_psychosis = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_scid5_schizotypal_personality_sciddpq.csv")
demos = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_sociodemographics.csv")
sofas_screening = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_sofas_screening.csv")
sofas_followup = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_sofas_followup.csv")
#speech_sampling_run_sheet
speech = pd.read_csv(input_path+"Prescient"+site+"/raw/"+id+"/surveys/"+id+"_speech_sampling_run_sheet.csv")

# All csvs have LastModifiedDate, subjectkey, interview_date, interview_age, gender, visit
# making a list of names for each of the forms
form_names = ['adverse_events', 'assist','bprs', 'cdss', 'coenrollment_form', 'current_health_status', 'daily_activity_and_saliva_sample_collection', 'digital_biomarkers_axivity_onboarding', 'digital_biomarkers_mindlamp_onboarding', 'eeg_run_sheet', 'entry_status', 'global_functioning_role_scale', 'global_functioning_social_scale', 'guid_form', 'health_conditions_genetics_fluid_biomarkers', 'health_conditions_medical_historypsychiatric_histo', 'inclusionexclusion_criteria_review', 'item_promis_for_sleep', 'lifetime_ap_exposure_screen', 'missing_data', 'mri_run_sheet', 'nsipr', 'oasis', 'penncnb', 'perceived_discrimination_scale', 'perceived_stress_scale', 'pgis', 'premorbid_adjustment_scale', 'premorbid_iq_reading_accuracy', 'psychosis_polyrisk_score', 'psychosocial_treatment_form', 'psychs_p1p8', 'pubertal_developmental_scale', 'ra_prediction', 'recruitment_source', 'scid5_schizotypal_personality_sciddpq', 'sociodemographics', 'sofas_screening', 'sofas_followup', 'speech_sampling_run_sheet']

# making a list of the dataframes
forms = [adverse, assist, bprs, cdss, coenrollment, current_health, daily_activity, dig_axivity, dig_mindlamp, eeg, entry_status, global_role, global_social, guid, health_biomarkers, health_history, inclusion, promis, lifetime, missing, mri, nsipr, oasis, penn, per_dis, per_stress, pgis, premorbidadjust, premorbidiq, psychosis, psychosocial, psychsp1p8, puberty, ra_pred, recruitment, scid_psychosis, demos, sofas_screening, sofas_followup, speech]

# creating a dataframe to store all the info
forms_info = pd.DataFrame(index = form_names, columns = ['Visit', 'Interview_date', 'Total_Variables', 'Percent_complete'])

dpdash_tot = pd.DataFrame(index = form_names, columns = ['Value'])
dpdash_percent = pd.DataFrame(index = form_names, columns = ['Value'])
dpdash_date = pd.DataFrame(index = form_names, columns = ['Value'])
dpdash_complete = pd.DataFrame(index = form_names, columns = ['Value'])
dpdash_missing = pd.DataFrame(index = form_names, columns = ['Value'])

for df,name in zip(forms,form_names):
	print(name)
	print(df.T)
	df = df.replace('-', np.NaN)	
	df = df.replace('na', np.NaN)
	df = df.replace('na.', np.NaN)
	df = df.replace('None', np.NaN)

	
	if 'visit' in df.columns:
		print('Visit: ', df.at[0, 'visit'])
		forms_info.at[name, 'Visit'] = df.at[0, 'visit']

	num_var = len(df.T) - 6
	print("Number of variables: ", num_var)

	missing = df.isnull().sum(axis=1)[0]
	print('Missing variables: ', missing)
	percent = 100 - round((missing/num_var)*100)

	#comp = df.columns[df.columns.str.endswith(tuple('_complete'))]
	comp = df.filter(regex = '(?:_complete)$' , axis=1)
	colu=0
	if not comp.empty:
		print(comp)
		dpdash_complete.at[name, 'Value'] = comp.iloc[0, colu]	

	if 'interview_date' in df.columns:
		forms_info.at[name, 'Interview_date'] = df.at[0, 'interview_date']
	
	forms_info.at[name, 'Total_Variables'] = num_var
	forms_info.at[name, 'Percent_complete'] = percent

	dpdash_tot.at[name, 'Value'] = num_var
	dpdash_percent.at[name, 'Value'] = percent

print(forms_info)

dpdash_percent.index = dpdash_percent.index.str.replace('(.*)', r'\1_perc') 
dpdash_tot.index = dpdash_tot.index.str.replace('(.*)', r'\1_tot') 
dpdash_complete.index = dpdash_complete.index.str.replace('(.*)', r'\1_complete') 

# Removing forms that are missing all of their data
dpdash_complete = dpdash_complete.dropna(axis=0)

print(dpdash_complete)
#print(dpdash_tot)

# concatenating all of the measures
frames = [dpdash_percent, dpdash_tot, dpdash_complete]
dp_con = pd.concat(frames)
#print(dp_con)

# reorganizing measures
dp_con = dp_con.sort_index(axis = 0)
dp_con = dp_con.transpose()
#print(dp_con)

names_dash = ['day', 'reftime', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']

dpdash_main = pd.DataFrame(columns = names_dash)
dpdash_main.at['Value', 'subjectid'] = id
dpdash_main.at['Value', 'site'] = site
dpdash_main.at['Value', 'mtime'] = inclusion.at[0, 'interview_date']
day = '1'
dpdash_main.at['Value', 'day'] = day

frames = [dpdash_main, dp_con]
dpdash_full = pd.concat(frames, axis=1)
print(dpdash_full.T)


# saving the csv
dpdash_full.to_csv("Prescient_status/formqc-"+id+"-percent-day1to"+day+".csv", sep=',', index = False, header=True)

dpdash_full.to_csv(output_path+"formqc-"+id+"-percent-day1to"+day+".csv", sep=',', index = False, header=True)






