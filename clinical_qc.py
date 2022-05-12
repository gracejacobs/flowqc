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

site=str(sys.argv[2])
print("Site: ", site)

#event = str(sys.argv[3])
#print("Event: ", event)
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
dict = pd.read_csv('CloneOfREDCapIIYaleRecords_DataDictionary_2022-05-11.csv',
                                sep= ",",
                                index_col = False, low_memory=False)

# Getting all the form names from the data dictionary
form_names = dict['Form Name'].unique()

# Subsetting data based on event
#events = sub_data.redcap_event_name.unique()
for event in event_list:
	print("Event: " + event)
	sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([event])]
	sub_data = sub_data.reset_index(drop=True)

	### Looping through each of the variables in each of the forms to calculate the total 		number of poten#tial variables, the variables that actually exist for the participant, 		and the % missing

	#creating dataframe for output data
	all_forms = pd.DataFrame(columns = form_names)
	col=0
	for name in form_names:
		col=col+1
		form = dict.loc[dict['Form Name'] == name]
		form_vars = form['Variable / Field Name'].tolist()
		#print('Number of variables/columns in form: ', len(form_vars))
		all_forms.at["Tot_variables", name] = len(form_vars)
	
		ex=0
		for var in form_vars:
			if var in sub_data:
				ex=ex+1 #print("Column", col, "exists in the DataFrame.")
		all_forms.at["Existing_variables", name] = ex

		miss=0
		for variable in form_vars:
			if (variable in sub_data) and sub_data[variable].isnull().values.any():
				miss=miss+1
		all_forms.at["Missing_vars", name] = miss	

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
	#print(all_forms)

	## Looping through to get whcih forms are marked as complete - currently not using
	forms_complete = pd.DataFrame(columns = form_names)
	col=0
	for name in form_names:
		col=col+1
		form = dict.loc[dict['Form Name'] == name]
		form_vars = form['Variable / Field Name'].tolist()

		# Add completed or not
		for var in form_vars:
			if 'complete' in var:
				print(var)
				#all_forms.at["Completed", name] = sub_data[var]
				forms_complete.at["Completed", name] = "yes"
			else:
				forms_complete.at["Completed", name] = "NA"

	#print(forms_complete.T)

	# Create dataframe for date of interview and date of data entry
	date_forms = pd.DataFrame(columns = form_names)
	col=0
	for name in form_names:
		col=col+1
		form = dict.loc[dict['Form Name'] == name]
		form_vars = form['Variable / Field Name'].tolist()

		for var in form_vars:
			if '_interview_date' in var:
				date_forms.at["Interview_date", name] = sub_data.at[0, var]
                       
		for var in form_vars:
			if '_entry_date' in var:
				date_forms.at["Entry_date", name] = sub_data.at[0, var]

	date_forms = date_forms.loc[:,~date_forms.columns.str.contains('^ sans-serif', case=False)]
	
	#print(date_forms.T)
	# Getting difference between dates & dropping forms without both dates
	date_forms = date_forms.dropna(axis=1)

	for col in date_forms:
		if type(date_forms.at["Interview_date", col]) == float and pd.isna(date_forms.at["Interview_date", col]):
			date_forms.at["Date_diff", col] = "no"
		else:
			d1 = date_forms.at["Entry_date", col]
			d2 = date_forms.at["Interview_date", col]
			date_forms.at["Date_diff", col] = days_between(d1, d2)

	date_forms = date_forms.add_suffix('_date')              
	#print(date_forms.T)
	# Removing forms that are missing data
	for col in all_forms:
		if all_forms.loc["Percentage", col] == 0:
			#all_forms = all_forms.drop(col, axis=1, inplace=True)
			del all_forms[col]

	# creating dpdash forms
	dpdash_percent = pd.DataFrame(all_forms.loc["Percentage"])
	dpdash_percent.index = dpdash_percent.index.str.replace('(.*)', r'\1_perc') 
	dpdash_percent.columns = [event]
	#print(dpdash_percent)

	dpdash_tot = pd.DataFrame(all_forms.loc["Existing_variables"])
	dpdash_tot.index = dpdash_tot.index.str.replace('(.*)', r'\1_tot')
	dpdash_tot.columns = [event]
	#print(dpdash_tot)

	dpdash_date = pd.DataFrame(date_forms.loc["Date_diff"])
	dpdash_date.columns = [event]
	#print(dpdash_date)

	# concatenating all of the measures
	frames = [dpdash_percent, dpdash_tot, dpdash_date]
	dp_con = pd.concat(frames)
	# reorganizing measures
	dp_con = dp_con.sort_index(axis = 0)
	dp_con = dp_con.transpose()
	#print(dp_con)

	names_dash = ['day', 'reftime', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
	dpdash_main = pd.DataFrame(columns = names_dash)
	dpdash_main.at[event, 'subjectid'] = id
	dpdash_main.at[event, 'site'] = site
	# want to change this to the interview date
	entry = date_forms.columns[1]
	dpdash_main.at[event, 'mtime'] = date_forms.at["Interview_date", entry]

	sub_consent = sub_data_all[sub_data_all['redcap_event_name'].isin(["screening_arm_1"])]
	d1 = sub_consent.at[0, "chric_consent_date"]
	d2 = date_forms.at["Interview_date", entry]
	dpdash_main.at[event, 'day'] = days_between(d1, d2) + 1

	frames = [dpdash_main, dp_con]
	dpdash_full = pd.concat(frames, axis=1)
	#print(dpdash_full.T)

	# Saving to a csv based on ID and event
	dpdash_full.to_csv("Pronet_status/"+event+"-"+site+"-"+id+"-formscheck.csv", sep=',', index = False, header=True)





## Looping through to get whcih forms have a missing table
#forms_missing = pd.DataFrame(columns = form_names)
#col=0
#for name in form_names:
#        col=col+1
#        form = dict.loc[dict['Form Name'] == name]
#        form_vars = form['Variable / Field Name'].tolist()
#
#        # Add missing data
#        for var in form_vars:
#                if 'missing' in var:
#                        if (var in sub_data) and sub_data[var].isnull().values.any():
#                        print(var)
#                        #all_forms.at["Completed", name] = sub_data[var]
#                        forms_complete.at["Missing", name] = "yes"
#                else:
#                        forms_complete.at["Missing", name] = "NA"
#
#print(forms_missing.T)














