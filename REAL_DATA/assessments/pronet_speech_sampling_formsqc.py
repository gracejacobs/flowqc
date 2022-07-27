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

id = str(sys.argv[1])
print("ID: ", id)
site = id[0:2]
print("Site: ", site)

output_path = "/data/predict/data_from_nda/formqc/"
print("Output path: ", output_path)

sub_data = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)
print("Participant json: ", sub_data)

with open(sub_data, 'r') as f:
	json = json.load(f)

sub_data_all = pd.DataFrame.from_dict(json, orient="columns")
#replacing empty cells with NaN
sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)

# Opening data dictionary
dict = pd.read_csv('/data/pnl/home/gj936/U24/Clinical_qc/flowqc/CloneOfREDCapIIYaleRecords_DataDictionary_2022-05-11.csv',
                                sep= ",",
                                index_col = False, low_memory=False)

print(sub_data_all)

#form_names = dict['Form Name'].unique()
#for n in form_names:
#	print(n)

#############################################################################################################
form_name = ['speech_sampling_run_sheet']
name = 'speech_sampling_run_sheet'
pre = 'chrspeech'
event_list = ["baseline_arm_1"]

# need to check if event in json and then append events to the file
# check if interview date, if not move on to next event


for event in event_list:
	print("Event: " + event) 
	sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([event])]
	sub_data = sub_data.reset_index(drop=True)

	#all_forms = pd.DataFrame(columns = form_names)
	#forms_missing = pd.DataFrame(columns = form_names)
	#date_forms = pd.DataFrame(columns = form_names)
	form_info = pd.DataFrame(columns = form_name)
	

	form = dict.loc[dict['Form Name'] == name]
	form_vars = form['Variable / Field Name'].tolist()
	#print('Number of variables/columns in form: ', len(form_vars))
	form_info.at["Tot_variables", name] = len(form_vars)

	# entry date
	var = pre + '_entry_date'
	form_info.at["Entry_date", name] = sub_data.at[0, var]

	# interview date
	var = pre + '_interview_date'
	form_info.at["Interview_date", name] = sub_data.at[0, var]
	
	#if (var in sub_data) and sub_data[var].isnull().values.any():
	#	print('no data for this form')
	#else:
	#	print('variable is not null')

	# calculating difference in dates between entry and interview
	d1 = form_info.at["Interview_date", name]
	d2 = form_info.at["Entry_date", name]
	form_info.at["Diff_date", name]  = days_between(d1, d2)

	# missing data
	var = pre + '_missing'
	form_info.at["Missing_data", name] = sub_data.at[0, var]

	# Calculating how many variables per form exist in the json
	ex=0
	for var in form_vars:
		if var in sub_data:
			ex=ex+1 
	form_info.at["Existing_variables", name] = ex
	#print("Existing variables: ", ex)

	# Calculating how many of those don't have values
	miss=0
	for variable in form_vars:
		if (variable in sub_data) and sub_data[variable].isnull().values.any():
			miss=miss+1
	form_info.at["Missing_vars", name] = miss
	#print("Missing_vars: ", miss)


	# Calculating the percentage of missing as compared to the existing variables
	for col in form_info:
		if form_info.at["Existing_variables", col] > 0:
			if form_info.at["Missing_vars", col] > 0:
				m=form_info.at["Missing_vars", col]
				e=form_info.at["Existing_variables", col]
				form_info.at["Percentage", col] = (100 - round((m / e) * 100))
			else:
				form_info.at["Percentage", col] = 100
		else:
			form_info.at["Percentage", col] = 0

		form_info = form_info.loc[:,~form_info.columns.str.contains('^ sans-serif', case=False)] 

	# conducted in english
	var = pre + '_english'
	form_info.at["English", name] = sub_data.at[0, var]

	# remote or not
	var = pre + '_remote_status'
	form_info.at["Remote", name] = sub_data.at[0, var]

	# zoom used
	var = pre + '_zoom'
	form_info.at["Zoom", name] = sub_data.at[0, var]

	# participant camera type
	var = pre + '_camera_type'
	form_info.at["Camera_type", name] = sub_data.at[0, var]

	# successfully uploaded
	var = pre + '_upload'
	form_info.at["Uploaded", name] = sub_data.at[0, var]

	# audio paused
	var = 'chr_speech_pause'
	form_info.at["Paused", name] = sub_data.at[0, var]

	# deviations
	var = pre + '_deviation'
	form_info.at["Deviations", name] = sub_data.at[0, var]

	# quality
	var = pre + '_quality'
	form_info.at["Quality", name] = sub_data.at[0, var]	

	#forms_missing = forms_missing.dropna(axis=1)	
	#print(form_info)  

	names_dash = ['reftime', 'day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
	dpdash_main = pd.DataFrame(columns = names_dash)
	dpdash_main.at[event, 'subjectid'] = id
	dpdash_main.at[event, 'site'] = site

	dpdash_main.at[event, 'mtime'] = form_info.at["Interview_date", name]
		
	# consent date is day 1
	sub_consent = sub_data_all[sub_data_all['redcap_event_name'].isin(["screening_arm_1"])]
			
	d1 = sub_consent.at[0, "chric_consent_date"]
	d2 = form_info.at["Interview_date", name]

	# setting day as the difference between the consent date (day 1) and interview date
	dpdash_main.at[event, 'day'] = (days_between(d1, d2) + 1)
	day = str(dpdash_main.at[event, 'day'])

	dpdash_main.set_axis([name], axis=0, inplace=True)
	form_info = form_info.T

	frames = [dpdash_main, form_info]
	dpdash_full = pd.concat(frames, axis=1)
	print(dpdash_full.T)

	# Saving to a csv based on ID and event
	dpdash_full.to_csv(output_path + site + '-' + id +"-form_" + name + "-day1to"+ day+ ".csv", sep=',', index = False, header=True)


#############################################################################################################
form_name = ['psychs_p9ac32']
name = 'psychs_p9ac32'
pre = 'chrpsychs_scr'
event_list = ["screening_arm_1"]

# need to check if event in json and then append events to the file
# check if interview date, if not move on to next event

for event in event_list:
	print("Event: " + event) 
	sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([event])]
	sub_data = sub_data.reset_index(drop=True)

	form_info = pd.DataFrame(columns = form_name)

	form = dict.loc[dict['Form Name'] == name]
	form_vars = form['Variable / Field Name'].tolist()
	#print('Number of variables/columns in form: ', len(form_vars))
	form_info.at["Tot_variables", name] = len(form_vars)

	# entry date
	var = pre + '_entry_date'
	form_info.at["Entry_date", name] = sub_data.at[0, var]

	# interview date
	var = pre + '_interview_date'
	form_info.at["Interview_date", name] = sub_data.at[0, var]

	# calculating difference in dates between entry and interview
	d1 = form_info.at["Interview_date", name]
	d2 = form_info.at["Entry_date", name]
	form_info.at["Diff_date", name]  = days_between(d1, d2)

	# missing data
	#var = pre + '_missing'
	#form_info.at["Missing_data", name] = sub_data.at[0, var]

	# Calculating how many variables per form exist in the json
	ex=0
	for var in form_vars:
		if var in sub_data:
			ex=ex+1 
	form_info.at["Existing_variables", name] = ex

	# Calculating how many of those don't have values
	miss=0
	for variable in form_vars:
		if (variable in sub_data) and sub_data[variable].isnull().values.any():
			miss=miss+1
	form_info.at["Missing_vars", name] = miss

	# Calculating the percentage of missing as compared to the existing variables
	for col in form_info:
		if form_info.at["Existing_variables", col] > 0:
			if form_info.at["Missing_vars", col] > 0:
				m=form_info.at["Missing_vars", col]
				e=form_info.at["Existing_variables", col]
				form_info.at["Percentage", col] = (100 - round((m / e) * 100))
			else:
				form_info.at["Percentage", col] = 100
		else:
			form_info.at["Percentage", col] = 0

		form_info = form_info.loc[:,~form_info.columns.str.contains('^ sans-serif', case=False)] 

	var = pre + '_ac1'
	form_info.at["Psychosis_dx", name] = sub_data.at[0, var]

	var = pre + '_ac2'
	form_info.at["CAARMS_BLIPS", name] = sub_data.at[0, var]

	var = pre + '_ac3'
	form_info.at["CAARMS_APS_subfreq", name] = sub_data.at[0, var]

	var = pre + '_ac4'
	form_info.at["CAARMS_APS_subint", name] = sub_data.at[0, var]

	var = pre + '_ac5'
	form_info.at["CAARMS_APS", name] = sub_data.at[0, var]

	var = pre + '_ac6'
	form_info.at["CAARMS_vulnerability", name] = sub_data.at[0, var]

	var = pre + '_ac7'
	form_info.at["CAARMS_UHR", name] = sub_data.at[0, var]

	var = pre + '_ac8'
	form_info.at["SIPS_BIPS_lifetime", name] = sub_data.at[0, var]	

	var = pre + '_ac9'
	form_info.at["SIPS_BIPS_progression", name] = sub_data.at[0, var]

	var = pre + '_ac10'
	form_info.at["SIPS_BIPS_persistence", name] = sub_data.at[0, var]

	var = pre + '_ac11'
	form_info.at["SIPS_BIPS_partial_rm", name] = sub_data.at[0, var]

	var = pre + '_ac12'
	form_info.at["SIPS_BIPS_remission", name] = sub_data.at[0, var]

	var = pre + '_ac13'
	form_info.at["SIPS_BIPS_current", name] = sub_data.at[0, var]

	var = pre + '_ac14'
	form_info.at["SIPS_APSS_lifetime", name] = sub_data.at[0, var]

	var = pre + '_ac15'
	form_info.at["SIPS_APSS_progression", name] = sub_data.at[0, var]

	var = pre + '_ac16'
	form_info.at["SIPS_APSS_persistence", name] = sub_data.at[0, var]

	var = pre + '_ac17'
	form_info.at["SIPS_APSS_parital_rm", name] = sub_data.at[0, var]

	var = pre + '_ac18'
	form_info.at["SIPS_APSS_remission", name] = sub_data.at[0, var]

	var = pre + '_ac19'
	form_info.at["SIPS_APSS_current", name] = sub_data.at[0, var]	

	var = pre + '_ac20'
	form_info.at["SIPS_GRD_lifetime", name] = sub_data.at[0, var]

	var = pre + '_ac21'
	form_info.at["SIPS_GRD_progression", name] = sub_data.at[0, var]

	var = pre + '_ac22'
	form_info.at["SIPS_GRD_persistence", name] = sub_data.at[0, var]

	var = pre + '_ac23'
	form_info.at["SIPS_GRD_parital_rm", name] = sub_data.at[0, var]

	var = pre + '_ac24'
	form_info.at["SIPS_GRD_remission", name] = sub_data.at[0, var]

	var = pre + '_ac25'
	form_info.at["SIPS_GRD_current", name] = sub_data.at[0, var]

	var = pre + '_ac26'
	form_info.at["SIPS_CHR_lifetime", name] = sub_data.at[0, var]

	var = pre + '_ac27'
	form_info.at["SIPS_CHR_progression", name] = sub_data.at[0, var]

	var = pre + '_ac28'
	form_info.at["SIPS_CHR_persistence", name] = sub_data.at[0, var]

	var = pre + '_ac29'
	form_info.at["SIPS_CHR_parital_rm", name] = sub_data.at[0, var]

	var = pre + '_ac30'
	form_info.at["SIPS_CHR_remission", name] = sub_data.at[0, var]

	var = pre + '_ac31'
	form_info.at["SIPS_CHR_current", name] = sub_data.at[0, var]	

	var = pre + '_ac32'
	form_info.at["DSM-5_attenuated_psychosis", name] = sub_data.at[0, var]
	
	# adding standard columns  
	names_dash = ['reftime', 'day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
	dpdash_main = pd.DataFrame(columns = names_dash)
	dpdash_main.at[event, 'subjectid'] = id
	dpdash_main.at[event, 'site'] = site

	dpdash_main.at[event, 'mtime'] = form_info.at["Interview_date", name]
		
	# consent date is day 1
	sub_consent = sub_data_all[sub_data_all['redcap_event_name'].isin(["screening_arm_1"])]
			
	d1 = sub_consent.at[0, "chric_consent_date"]
	d2 = form_info.at["Interview_date", name]

	# setting day as the difference between the consent date (day 1) and interview date
	dpdash_main.at[event, 'day'] = (days_between(d1, d2) + 1)
	day = str(dpdash_main.at[event, 'day'])

	dpdash_main.set_axis([name], axis=0, inplace=True)
	form_info = form_info.T

	frames = [dpdash_main, form_info]
	dpdash_full = pd.concat(frames, axis=1)
	print(dpdash_full.T)

	# Saving to a csv based on ID and event
	dpdash_full.to_csv(output_path + site + '-' + id +"-form_" + name + "-day1to"+ day+ ".csv", sep=',', index = False, header=True)






















