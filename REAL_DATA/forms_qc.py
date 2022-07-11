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

### Requires three arguements - ID number (YA00009), site (e.g., YA) and the event (baseline_arm_1) at which you want to generate a table of available/inputed data

# Tot_variables = the total number of possible variables based on the REDCap data dictionary
# Existing_variables = the number of variables in that participant's json
# Missing_vars = the number of variables with NA
# Percentage = the percentage of variables NOT missing (based on existing variables) for that participant

id = str(sys.argv[1])
print("ID: ", id)

site = id[0:2]
print("Site: ", site)

#event = str(sys.argv[3])
#print("Event: ", event)
#event_list = ["baseline_arm_1"]
event_list = ["screening_arm_1", "baseline_arm_1"]

sub_data = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)
print("Participant json: ", sub_data)

with open(sub_data, 'r') as f:
	json = json.load(f)

sub_data_all = pd.DataFrame.from_dict(json, orient="columns")
#replacing empty cells with NaN
sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)

# Opening data dictionary
#dict = pd.read_csv('AMPSCZFormRepository_DataDictionary_2022-04-02.csv',
dict = pd.read_csv('/data/pnl/home/gj936/U24/Clinical_qc/flowqc/CloneOfREDCapIIYaleRecords_DataDictionary_2022-05-11.csv',
                                sep= ",",
                                index_col = False, low_memory=False)

# Getting all the form names from the data dictionary
#form_names = pd.read_csv('form_names.csv')
form_names = dict['Form Name'].unique()


# Subsetting data based on event
#events = sub_data.redcap_event_name.unique()
for event in event_list:
	print("Event: " + event)
	sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([event])]
	sub_data = sub_data.reset_index(drop=True)

	print(sub_data)

	### Looping through each of the variables in each of the forms to calculate the total 		number of poten#tial variables, the variables that actually exist for the participant, 		and the % missing

	#creating dataframe for output data
	all_forms = pd.DataFrame(columns = form_names)
	forms_missing = pd.DataFrame(columns = form_names)
	date_forms = pd.DataFrame(columns = form_names)

	col=0
	for name in form_names:
		#print(name)
		col=col+1
		form = dict.loc[dict['Form Name'] == name]
		form_vars = form['Variable / Field Name'].tolist()

		all_forms.at["Tot_variables", name] = len(form_vars)
		
		# Calculating how many variables per form exist in the json
		ex=0
		for var in form_vars:
			if var in sub_data:
				#print(var)
				#print(sub_data.at[0, var])
				ex=ex+1 #print("Column", col, "exists in the DataFrame.")
		all_forms.at["Existing_variables", name] = ex
		#print("Existing_vars: ", ex)

		# Calculating how many of those don't have values
		miss=0
		for variable in form_vars:
			if (variable in sub_data) and sub_data[variable].isnull().values.any():
				miss=miss+1
		all_forms.at["Missing_vars", name] = miss
		#print("Missing_vars: ", miss)

		#checking if there is information about missing data
		for var in form_vars:
			if 'missing_spec' in var and (var in sub_data):
				#print(var)
				forms_missing.at["Missing", name] = sub_data.at[0, var]
		
		# fill dataframe for date of interview and date of data entry 
		for var in form_vars:
			if '_interview_date' in var and (var in sub_data):
				date_forms.at["Interview_date", name] = sub_data.at[0, var]
                       
		for var in form_vars:
			if '_entry_date' in var and (var in sub_data):
				date_forms.at["Entry_date", name] = sub_data.at[0, var]


	#print(date_forms.T)
	#print(forms_missing)

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

	forms_missing = forms_missing.dropna(axis=1)	
	print(all_forms.T)  

	# Removing forms that are missing all of their data
	for col in all_forms:
		if all_forms.loc["Percentage", col] == 0:
			#all_forms = all_forms.drop(col, axis=1, inplace=True)
			del all_forms[col]       	

	# Getting difference between dates & dropping forms without both dates
	date_forms = date_forms.dropna(axis=1)
	print(date_forms.T)

	#removing the time from

	date_forms = date_forms.replace(' 09:16', '', regex=True)
	date_forms = date_forms.replace(' 14:48', '', regex=True)
	date_forms = date_forms.replace(' 11:12', '', regex=True)
	date_forms = date_forms.replace(' 13:50', '', regex=True)
	#date_forms['Interview_date'] = date_forms['Interview_date'].apply(extract_date)
	#date_forms['Interview_date'] = date_forms['Interview_date'].str.split(' ').str[0]

	date_forms = date_forms.replace(' 12:40', '', regex=True)
	
	print(date_forms.T)


	for col in date_forms:
		if type(date_forms.at["Interview_date", col]) == float and pd.isna(date_forms.at["Interview_date", col]):
			date_forms.at["Date_diff", col] = "no"
		else:
			d1 = date_forms.at["Entry_date", col]
			d2 = date_forms.at["Interview_date", col]
			date_forms.at["Date_diff", col] = days_between(d1, d2)

	date_forms = date_forms.add_suffix('_date')              
	#print(date_forms.T)

	# Adding the _complete variable for each of the forms
	completed = pd.DataFrame(columns = ['Variable', 'Value'])
	ro=0
	for col in sub_data:
		#if '_complete' in col:
		if col.endswith('_complete'):
			ro=ro+1
			completed.at[ro, 'Variable'] = col
			completed.at[ro, 'Value'] = sub_data.at[0, col]
			#completed = completed.append(df, ignore_index=True)

	completed = completed.dropna(axis=0)
	completed = completed.set_index('Variable')
	#print(completed)
	
	# creating dpdash forms
	dpdash_percent = pd.DataFrame(all_forms.loc["Percentage"])
	dpdash_percent.index = dpdash_percent.index.str.replace('(.*)', r'\1_perc') 
	dpdash_percent.columns = [event]
	#print(dpdash_percent)

	dpdash_tot = pd.DataFrame(all_forms.loc["Existing_variables"])
	dpdash_tot.index = dpdash_tot.index.str.replace('(.*)', r'\1_tot')
	dpdash_tot.columns = [event]
	#print(dpdash_tot)

	dpdash_miss = pd.DataFrame(forms_missing.loc["Missing"])
	dpdash_miss.index = dpdash_miss.index.str.replace('(.*)', r'\1_miss')
	dpdash_miss.columns = [event]

	dpdash_date = pd.DataFrame(date_forms.loc["Date_diff"])
	dpdash_date.columns = [event]
	#print(dpdash_date)

	# completed data
	completed.columns = [event]
	#print(completed)

	# concatenating all of the measures
	frames = [dpdash_percent, dpdash_tot, dpdash_miss, dpdash_date, completed]
	dp_con = pd.concat(frames)

	# reorganizing measures
	dp_con = dp_con.sort_index(axis = 0)
	#print(dp_con)
	dp_con = dp_con.transpose()
	print(dp_con)

	names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
	dpdash_main = pd.DataFrame(columns = names_dash)
	dpdash_main.at[event, 'subjectid'] = id
	dpdash_main.at[event, 'site'] = site
	# want to change this to the interview date
	entry = date_forms.columns[1]
	dpdash_main.at[event, 'mtime'] = date_forms.at["Interview_date", entry]

	sub_consent = sub_data_all[sub_data_all['redcap_event_name'].isin(["screening_arm_1"])]
	d1 = sub_consent.at[0, "chric_consent_date"]
	d2 = date_forms.at["Interview_date", entry]
	# setting day as the difference between the consent date (day 1) and interview date
	day = days_between(d1, d2) + 1
	dpdash_main.at[event, 'day'] = day

	frames = [dpdash_main, dp_con]
	dpdash_full = pd.concat(frames, axis=1)
	#print(dpdash_full.T)

	# Saving to a csv based on ID and event
	dpdash_full.to_csv("Pronet_status/"+event+"-"+site+"-"+id+"-formscheck.csv", sep=',', index = False, header=True)





