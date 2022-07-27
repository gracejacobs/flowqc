import sys
import numpy as np
import pandas as pd
import argparse
import json
import os
from datetime import datetime

def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

### Requires three arguements - ID number (YA00009), site (e.g., YA) and the event (baseline_arm_1) at which you want to generate a table of available/inputed data

# Tot_variables = the total number of possible variables based on the REDCap data dictionary
# Existing_variables = the number of variables in that participant's json
# Missing_vars = the number of variables with NA
# Percentage = the percentage of variables NOT missing (based on existing variables) for that participant

id = str(sys.argv[1])
print("ID: ", id)

site = id[0:2]
print("Site: ", site)

output1 = "/data/predict/data_from_nda/formqc/"
output_processed = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/processed/{1}/surveys/".format(site, id)

# creating a folder if there isn't one
if not os.path.exists(output_processed):
	print("Creating output directory in participant's processed directory")
	os.makedirs(output_processed)

sub_data = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)
print("Participant json: ", sub_data)

with open(sub_data, 'r') as f:
	json = json.load(f)

sub_data_all = pd.DataFrame.from_dict(json, orient="columns")
#replacing empty cells with NaN
sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)

# Opening data dictionary
#dict = pd.read_csv('AMPSCZFormRepository_DataDictionary_2022-04-02.csv',
dict = pd.read_csv('/data/pnl/home/gj936/U24/Clinical_qc/flowqc/CloneOfREDCapIIYaleRecords_DataDictionary_2022-07-27.csv',
                                sep= ",",
                                index_col = False, low_memory=False)

# Getting all the form names from the data dictionary
#form_names = pd.read_csv('form_names.csv')
form_names = dict['Form Name'].unique()
for i in form_names:
	print(i)
#form_names = form_names.remove('psychs_p1p8_fu_draft')

all_events = sub_data_all['redcap_event_name'].unique()
print(all_events)

#form_names = ['informed_consent_run_sheet', 'inclusionexclusion_criteria_review', 'recruitment_source', 'coenrollment_form', 'sofas_screening', 'mri_run_sheet', 'sociodemographics', 'lifetime_ap_exposure_screen']


# Subsetting data based on event
#events = sub_data.redcap_event_name.unique()
event_list = ["screening_arm_1", "baseline_arm_1", "month_1_arm_1", "month_2_arm_1"]

for name in form_names:
	form_tracker = {}
	day_tracker = []

	for event in event_list:
		print(name)
		print("Event: " + event)
		sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([event])]
		sub_data = sub_data.reset_index(drop=True)

		#print(sub_data)


		form_info = pd.DataFrame(columns = ['Variables'])
		form = dict.loc[dict['Form Name'] == name]
		form_vars = form['Variable / Field Name'].tolist()

		# Adding all variables to the csv
		for var in form_vars:
			if var in sub_data:
				form_info.at[var, 'Variables'] = sub_data.at[0, var]
	
		form_info.at["Total_variables", 'Variables'] = len(form_vars)
		
		# Calculating how many variables per form exist in the json
		ex=0
		for var in form_vars:
			if var in sub_data:
				ex=ex+1 #print("Column", col, "exists in the DataFrame.")
		form_info.at["Existing_variables", 'Variables'] = ex

		# Calculating how many of those don't have values
		miss=0
		for variable in form_vars:
			if (variable in sub_data) and sub_data[variable].isnull().values.any():
				miss=miss+1
		form_info.at["Missing_vars", 'Variables'] = miss
		
		# Calculating the percentage of missing as compared to the existing variables
		if form_info.at["Existing_variables", 'Variables'] > 0:
			if form_info.at["Missing_vars", 'Variables'] > 0:
				m=form_info.at["Missing_vars", 'Variables']
				e=form_info.at["Existing_variables", 'Variables']
				form_info.at["Percentage", 'Variables'] = (100 - round((m / e) * 100))
			else:
				form_info.at["Percentage", 'Variables'] = 100
		else:
			form_info.at["Percentage", 'Variables'] = 0

		#all_forms = all_forms.loc[:,~all_forms.columns.str.contains('^ sans-serif', case=False)] 
		
		# rounding varibles
		#for i in range(0, len(form_info.index)-1):	
		#	var = form_info.at[i, 'Variables'] 	
		#	if type(var) == float:
		#		form_info.at[i, 'Variables'] = round(var, decimals = 2)
				

		#form_info['Variables'].round(decimals = 2)

		form_info = form_info.transpose()
		#print(form_info)

		names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
		dpdash_main = pd.DataFrame(columns = names_dash)
		dpdash_main.at[event, 'subjectid'] = id
		dpdash_main.at[event, 'site'] = site

		sub_consent = sub_data_all[sub_data_all['redcap_event_name'].isin(["screening_arm_1"])]
		consent = sub_consent.at[0, "chric_consent_date"]

		# want to change this to the interview date
		int_avail = [s for s in form_vars if "_interview_date" in s]
		print("Int avail: ", int_avail)
		ent_avail = [s for s in form_vars if "_entry_date" in s]
		print("Ent avail: ", ent_avail)

		#print(form_info.T)

		# if interview is available and in the data
		if len(int_avail) > 0 and int_avail[0] in sub_data and name not in ['mri_run_sheet', 'current_health_status'] :
			print('Use interview date')
			var = int_avail[0]
			int_date = sub_data.at[0, var]
			if pd.isna(int_date):
				print('interview date is Nan')
			else:
				dpdash_main.at[event, 'mtime'] = int_date
				day = days_between(consent, int_date) + 1
		else:
			print("No interview date")
			if event in ['screening_arm_1']: # make day = 1
				dpdash_main.at[event, 'mtime'] = consent
				day = 1
			else:
				day = 2

		# use entry date for a couple forms that interview date doesn't work
		if name in ['mri_run_sheet', 'current_health_status'] and ent_avail[0] in sub_data:
			print("need entry date")
			var = ent_avail[0]
			ent_date = sub_data.at[0, var]
			if pd.isna(ent_date):
				print('entry date is Nan')
			else:
	
				dpdash_main.at[event, 'mtime'] = ent_date
				day = days_between(consent, ent_date) + 1
				

		# setting day as the difference between the consent date (day 1) and interview date
		dpdash_main.at[event, 'day'] = day

		dpdash_main = dpdash_main.reset_index(drop=True)
		form_info = form_info.reset_index(drop=True)

		frames = [dpdash_main, form_info]
		dpdash_full = pd.concat(frames, axis=1)
		#print(dpdash_full.T)

		day_tracker.append(str(day)) 

		form_tracker["Form_event_{0}".format(event)] = dpdash_full


	#print(form_tracker.keys())
	#print(form_tracker[Form_event_screening_arm_1])
	#print(form_tracker[0])
	#print(form_tracker["Form_event_screening_arm_1"])

	if day_tracker[-1] == 1:	
		last_day = 1
	else:
		last_day = day_tracker[-1]

	print(last_day)
	
	frames = [form_tracker["Form_event_screening_arm_1"], form_tracker["Form_event_baseline_arm_1"], form_tracker["Form_event_month_1_arm_1"], form_tracker["Form_event_month_2_arm_1"]]

	final_csv = pd.concat(frames)
	print(name)
	#removing rows with no data
	final_csv = final_csv[final_csv.Percentage != 0]
	print("FINAL CSV TO BE EXPORTED:\n", final_csv.T)
	
	# Saving to a csv based on ID and event
	final_csv.to_csv(output1 + site+"-"+id+"-form_"+name+'-day1to'+last_day+".csv", sep=',', index = False, header=True)

	final_csv.to_csv(output_processed + site+"-"+id+"-form_"+name+'-day1to'+last_day+".csv", sep=',', index = False, header=True)





