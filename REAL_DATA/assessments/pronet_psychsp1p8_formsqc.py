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

event_list = ["screening_arm_1", "baseline_arm_1"]

sub_data = "/data/predict/data_from_nda_dev/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)
print("Participant json: ", sub_data)

with open(sub_data, 'r') as f:
	json = json.load(f)

sub_data_all = pd.DataFrame.from_dict(json, orient="columns")
#replacing empty cells with NaN
sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)

# Opening data dictionary
dict = pd.read_csv('../CloneOfREDCapIIYaleRecords_DataDictionary_2022-05-11.csv',
                                sep= ",",
                                index_col = False, low_memory=False)

######## Getting all the form names from the data dictionarys
#form_names = dict['Form Name'].unique()
#form_names = pd.DataFrame(form_names)
#form_names.to_csv('form_names.csv', sep=',', index = False, header=False)
form_names = pd.read_csv('../form_names.csv', sep= ",")


form_names = ['item_promis_for_sleep']

for event in event_list:
	print("Event: " + event)
	sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([event])]
	sub_data = sub_data.reset_index(drop=True)

	all_forms = pd.DataFrame(columns = form_names)
	forms_missing = pd.DataFrame(columns = form_names)
	date_forms = pd.DataFrame(columns = form_names)
	
	print(sub_data)

	col=0
	for name in form_names:
		print(name)
		col=col+1
		form = dict.loc[dict['Form Name'] == name]
		form_vars = form['Variable / Field Name'].tolist()
		print('Number of variables/columns in form: ', len(form_vars))
		all_forms.at["Tot_variables", name] = len(form_vars)

		for variable in form_vars:
			if (variable in sub_data):
				print(variable)
				print(sub_data.at[0, variable])
		
		# Calculating how many variables per form exist in the json
		ex=0
		for var in form_vars:
			if var in sub_data:
				ex=ex+1 
		all_forms.at["Existing_variables", name] = ex
		print("Existing variables: ", ex)

		# Calculating how many of those don't have values
		miss=0
		for variable in form_vars:
			if (variable in sub_data) and sub_data[variable].isnull().values.any():
				miss=miss+1
		all_forms.at["Missing_vars", name] = miss
		print("Missing_vars: ", miss)

		#checking if there is information about missing data
		for var in form_vars:
			if 'missing_spec' in var and var in sub_data:
				print(var)
				forms_missing.at["Missing", name] = sub_data.at[0, var]
		
		# fill dataframe for date of interview and date of data entry 
		for var in form_vars:
			if '_interview_date' in var:
				date_forms.at["Interview_date", name] = sub_data.at[0, var]
			else:
		 		date_forms.at["Interview_date", name] = np.nan

		for var in form_vars:
			if '_entry_date' in var:
				date_forms.at["Entry_date", name] = sub_data.at[0, var]

		print(date_forms.T)

		# Calculating the percentage of missing as compared to the existing variables
		for col in all_forms:
			if all_forms.at["Existing_variables", col] > 0:
				if all_forms.at["Missing_vars", col] > 0:
					m=all_forms.at["Missing_vars", col]
					e=all_forms.at["Existing_variables", col]
					all_forms.at["Percentage", col] = (100 - round((m / e) * 100))
				else:
					all_forms.at["Percentage", col] = 100
			else:
				all_forms.at["Percentage", col] = 0

		all_forms = all_forms.loc[:,~all_forms.columns.str.contains('^ sans-serif', case=False)] 
		date_forms = date_forms.loc[:,~date_forms.columns.str.contains('^ sans-serif', case=False)]

		#forms_missing = forms_missing.dropna(axis=1)	
		print(all_forms.T)  

		# Removing forms that are missing all of their data
		for col in all_forms:
			if all_forms.loc["Percentage", col] == 0:
				del all_forms[col]       	

		# Getting difference between dates & dropping forms without both dates
		#date_forms = date_forms.dropna(axis=1)
		#removing the time from
		date_forms = date_forms.replace(' 09:16', '', regex=True)
		date_forms = date_forms.replace(' 14:48', '', regex=True)
		date_forms = date_forms.replace(' 11:12', '', regex=True)
		date_forms = date_forms.replace(' 13:50', '', regex=True)
	
		print(date_forms.T)
		#type(date_forms.at["Interview_date", col]) == float and 

		for col in date_forms:
			if pd.isna(date_forms.at["Interview_date", col]):
				print('Missing interview date')
				date_forms.at["Date_diff", col] = "missing"
			else:
				d1 = date_forms.at["Entry_date", col]
				d2 = date_forms.at["Interview_date", col]
				date_forms.at["Date_diff", col] = days_between(d1, d2)

		date_forms = date_forms.add_suffix('_date') 
		print(date_forms.T)             

		# Adding the _complete variable for each of the forms
		completed = pd.DataFrame(columns = ['Variable', 'Value'])
		for var in form_vars:
			if var.endswith('mri_complete') and var in sub_data:
				completed.at[0, 'Variable'] = var
				completed.at[0, 'Value'] = sub_data.at[0, var]

		#completed = completed.dropna(axis=0)
		completed = completed.set_index('Variable')
		print(completed.T)
	
		# creating dpdash forms
		dpdash_percent = pd.DataFrame(all_forms.loc["Percentage"])
		dpdash_percent.index = dpdash_percent.index.str.replace('(.*)', r'\1_perc') 
		dpdash_percent.columns = [event]
		print(dpdash_percent.T)

		dpdash_tot = pd.DataFrame(all_forms.loc["Existing_variables"])
		dpdash_tot.index = dpdash_tot.index.str.replace('(.*)', r'\1_tot')
		dpdash_tot.columns = [event]
		print("Total: ", dpdash_tot.T)

		dpdash_miss = pd.DataFrame(forms_missing.loc["Missing"])
		dpdash_miss.index = dpdash_miss.index.str.replace('(.*)', r'\1_miss')
		dpdash_miss.columns = [event]
		print("Miss: ", dpdash_miss.T)

		dpdash_date = pd.DataFrame(date_forms.loc["Date_diff"])
		dpdash_date.columns = [event]
		print("Date: ", dpdash_date.T)

		completed.columns = [event]

		# concatenating all of the measures
		frames = [dpdash_percent, dpdash_tot, dpdash_miss, dpdash_date, completed]
		dp_con = pd.concat(frames)
		print(dp_con)

		# reorganizing measures
		dp_con = dp_con.sort_index(axis = 0)
		dp_con = dp_con.transpose()

		names_dash = ['day', 'reftime', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
		dpdash_main = pd.DataFrame(columns = names_dash)
		dpdash_main.at[event, 'subjectid'] = id
		dpdash_main.at[event, 'site'] = site

		# want to change this to the interview date
		#entry = date_forms.columns[1]
		if pd.isna(date_forms.at["Entry_date", col]):
			dpdash_main.at[event, 'day'] = "unknown"

		else:
			dpdash_main.at[event, 'mtime'] = date_forms.at["Entry_date", 0]

			sub_consent = sub_data_all[sub_data_all['redcap_event_name'].isin(["screening_arm_1"])]
			
			d1 = sub_consent.at[0, "chric_consent_date"]
			d2 = date_forms.at["Entry_date", 0]

			# setting day as the difference between the consent date (day 1) and interview date
			dpdash_main.at[event, 'day'] = days_between(d1, d2) + 1

		frames = [dpdash_main, dp_con]
		dpdash_full = pd.concat(frames, axis=1)
		print(dpdash_full.T)

		# Saving to a csv based on ID and event
		dpdash_full.to_csv("../Pronet_status/"+event+"-"+site+"-"+id+"-"+name+".csv", sep=',', index = False, header=True)





















