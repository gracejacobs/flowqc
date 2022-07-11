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

output_path = "/data/predict/data_from_nda_dev/formqc/"
print("Output path: ", output_path)

event_list = ["baseline_arm_1"]

sub_data = "/data/predict/data_from_nda_dev/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)
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

######## Getting all the form names from the data dictionarys
#form_names = dict['Form Name'].unique()
#form_names = pd.DataFrame(form_names)
#form_names.to_csv('form_names.csv', sep=',', index = False, header=False)
form_names = pd.read_csv('/data/pnl/home/gj936/U24/Clinical_qc/flowqc/form_names.csv', sep= ",")


form_names = ['mri_run_sheet']

for event in event_list:
	print("Event: " + event)
	sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([event])]
	sub_data = sub_data.reset_index(drop=True)

	all_forms = pd.DataFrame(columns = form_names)
	forms_missing = pd.DataFrame(columns = form_names)
	date_forms = pd.DataFrame(columns = form_names)
	form_info = pd.DataFrame(columns = form_names)
	
	#print(sub_data)

	col=0
	for name in form_names:
		#print(name)
		col=col+1
		form = dict.loc[dict['Form Name'] == name]
		form_vars = form['Variable / Field Name'].tolist()
		print('Number of variables/columns in form: ', len(form_vars))
		form_info.at["Tot_variables", name] = len(form_vars)

		# test to see data
		#for variable in form_vars:
		#	if (variable in sub_data):
		#		print(variable)
		#		print(sub_data.at[0, variable])
		
		# adding entry date
		for var in form_vars:
			if '_entry_date' in var:
				form_info.at["Entry_date", name] = sub_data.at[0, var]

		# Calculating how many variables per form exist in the json
		ex=0
		for var in form_vars:
			if var in sub_data:
				ex=ex+1 
		form_info.at["Existing_variables", name] = ex
		print("Existing variables: ", ex)

		# Calculating how many of those don't have values
		miss=0
		for variable in form_vars:
			if (variable in sub_data) and sub_data[variable].isnull().values.any():
				miss=miss+1
		form_info.at["Missing_vars", name] = miss
		print("Missing_vars: ", miss)

		#checking if there is information about missing data
		for var in form_vars:
			if 'missing_spec' in var and var in sub_data:
				print(var)
				form_info.at["Missing_spec", name] = sub_data.at[0, var]


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

		#forms_missing = forms_missing.dropna(axis=1)	
		print(form_info)  

		names_dash = ['reftime', 'day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
		dpdash_main = pd.DataFrame(columns = names_dash)
		dpdash_main.at[event, 'subjectid'] = id
		dpdash_main.at[event, 'site'] = site

		dpdash_main.at[event, 'mtime'] = form_info.at["Entry_date", name]
		
		# consent date is day 1
		sub_consent = sub_data_all[sub_data_all['redcap_event_name'].isin(["screening_arm_1"])]
			
		d1 = sub_consent.at[0, "chric_consent_date"]
		d2 = form_info.at["Entry_date", name]

		# setting day as the difference between the consent date (day 1) and interview date
		dpdash_main.at[event, 'day'] = days_between(d1, d2) + 1
		day = str(dpdash_main.at[event, 'day'])

		dpdash_main.set_axis([name], axis=0, inplace=True)
		form_info = form_info.T
	
		print(dpdash_main)
		print(form_info)

		frames = [dpdash_main, form_info]
		dpdash_full = pd.concat(frames, axis=1)
		print(dpdash_full.T)

		# Saving to a csv based on ID and event
		dpdash_full.to_csv("/data/pnl/home/gj936/U24/Clinical_qc/flowqc/Pronet_status/"+event+"-"+site+"-"+id+"-"+name+".csv", sep=',', index = False, header=True)
		
		dpdash_full.to_csv(output_path + "formqc-" + id +"-" + name + "-day1to"+ day+ ".csv", sep=',', index = False, header=True)





















