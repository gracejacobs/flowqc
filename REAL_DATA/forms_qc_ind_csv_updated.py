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

# getting today's date
today = date.today()
today = today.strftime("%Y-%m-%d")

id = str(sys.argv[1])
print("ID: ", id)

site = id[0:2]
print("Site: ", site)

# chr or healthy
#sub_type = str(sys.argv[2])

output1 = "/data/predict/data_from_nda/formqc/"
output_processed = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/processed/{1}/surveys/".format(site, id)

# creating a folder if there isn't one
if not os.path.exists(output_processed):
	print("Creating output directory in participant's processed directory")
	#os.makedirs(output_processed)

sub_data = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)
print("Participant json: ", sub_data)

with open(sub_data, 'r') as f:
	json = json.load(f)

sub_data_all = pd.DataFrame.from_dict(json, orient="columns")
#replacing empty cells with NaN
sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)

#form_names = ['informed_consent_run_sheet', 'inclusionexclusion_criteria_review', 'recruitment_source', 'coenrollment_form', 'sofas_screening', 'mri_run_sheet', 'sociodemographics', 'lifetime_ap_exposure_screen']

# Subsetting data based on event
event_list = sub_data_all.redcap_event_name.unique()
screening = event_list[0]
print("Screening visit: " + screening)

# Opening data dictionary
dict = pd.read_csv('/data/pnl/home/gj936/U24/Clinical_qc/flowqc/AMPSCZFormRepository_DataDictionary_2022-08-19_min.csv', sep= ",", index_col = False, low_memory=False)

# Getting all the form names from the data dictionary
form_names = dict['Form Name'].unique()

percentage_form_2 = pd.DataFrame(columns = form_names)
#print(percentage_form)

sub_consent = sub_data_all[sub_data_all['redcap_event_name'].isin([screening])]
consent = sub_consent.at[0, "chric_consent_date"]
print("Consent date:" + consent)

print(id)
#form_names = ["inclusionexclusion_criteria_review"]
#Looping through forms to get form data
for name in form_names:
	form_tracker = {}
	day_tracker = []

	form = dict.loc[dict['Form Name'] == name]
	form_vars = form['Variable / Field Name'].tolist()

	print(id)
	print(name)

	form_info_2 = pd.DataFrame(columns = [event_list])

	ex=0
	miss=0
	for var in form_vars:
		if var in sub_data_all:				
			for event_2 in event_list:
				sub_data_test = sub_data_all[sub_data_all['redcap_event_name'].isin([event_2])]
				sub_data_test = sub_data_test.reset_index(drop=True)
				form_info_2.at[var, event_2] = sub_data_test.at[0, var]
				#df.iat[3, df.columns.get_loc('Remarks')] = 'No stock available. Will be available in 5 days'	
	for event_2 in event_list:
		form_info_2.at["Existing_variables", event_2] = 0
		form_info_2.at["Total_variables", event_2] = 0
		form_info_2.at["Missing_vars", event_2] = 0

		form_info_2.at["Existing_variables", event_2] = (len(form_info_2.index) - 3)
		form_info_2.at["Total_variables", event_2] = (len(form_vars) - 3)
		form_info_2.at["Missing_vars", event_2] = form_info_2.isna().sum()
			
		#print(form_info_2)
		# Calculating the percentage of missing as compared to the existing variables
	for event_2 in event_list:
		if form_info_2.loc["Existing_variables", event_2].values[0] > 0:
			if form_info_2.loc["Missing_vars", event_2].values[0] > 0:

				m=form_info_2.loc["Missing_vars", event_2].values[0]
				e=form_info_2.loc["Existing_variables", event_2].values[0]
				form_info_2.at["Percentage", event_2] = (100 - round((m / e) * 100))

			else:
				form_info_2.at["Percentage", event_2] = 100
		else:
			form_info_2.at["Percentage", event_2] = 0


	# make csv with form name, percentage, event
	for event_2 in event_list:
		percentage_form_2.at[event_2, name] = form_info_2.loc["Percentage", event_2].values[0]

	#print(percentage_form_2.T)
	# creating a single exclusion inclusion variable
	if name in ['inclusionexclusion_criteria_review']: 
		if pd.notna(sub_consent.at[0, "chrcrit_included"]) and sub_consent.loc[0, "chrcrit_included"] == "1":
			print("Participant meets inclusion criteria")
			form_info_2.at["included_excluded", screening] = 1
		
		if pd.notna(sub_consent.loc[0, "chrcrit_excluded"]) and sub_consent.loc[0, "chrcrit_excluded"] == "0":
			form_info_2.at["included_excluded", screening] = 0

	#print("The person is included or not:")
	#print(form_info_2.loc["included_excluded", screening])
		
	form_info_2 = form_info_2.transpose()
	form_info_2.reset_index(inplace=True)
	#print("Printing transposed form info")
	#print(form_info_2.T)

	#print("Setting up dpdash columns")
	names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
	dpdash_all = pd.DataFrame(columns = names_dash)

	#print(dpdash_all)
	frames = [dpdash_all, form_info_2]
	dpdash_main = pd.concat(frames)
	
	#print(dpdash_main.T)

	dpdash_main = dpdash_main.set_index('level_0')

	for event_2 in event_list:
		dpdash_main.at[event_2, 'subjectid'] = id
		dpdash_main.at[event_2, 'site'] = site

		# want to change this to the interview date
		int_avail = [s for s in form_vars if "_interview_date" in s]
		ent_avail = [s for s in form_vars if "_entry_date" in s]

		# if interview is available and in the data
		if len(int_avail) > 0 and int_avail[0] in sub_data_all and name not in ['mri_run_sheet', 'current_health_status'] :
			#print('Use interview date')
			var = int_avail[0]
			sub_data_test = sub_data_all[sub_data_all['redcap_event_name'].isin([event_2])]
			sub_data_test = sub_data_test.reset_index(drop=True)
			int_date = sub_data_test.at[0, var]

			if pd.notna(int_date):
				dpdash_main.at[event_2, 'mtime'] = int_date
				day = days_between(consent, int_date) + 1
		else:
			#print("No interview date")
			if event_2 in ['screening_arm_1']: # make day = 1
				dpdash_main.at[event_2, 'mtime'] = consent
				day = 1
			else:
				day = 2

		# use entry date for a couple forms that interview date doesn't work
		# check coenrollment_form
		if name in ['mri_run_sheet', 'current_health_status'] and ent_avail[0] in sub_data_all:
			#print("need entry date")
			var = ent_avail[0]
			sub_data_test = sub_data_all[sub_data_all['redcap_event_name'].isin([event_2])]
			sub_data_test = sub_data_test.reset_index(drop=True)
			ent_date = sub_data_test.at[0, var]
			
			if pd.notna(ent_date):
				dpdash_main.at[event_2, 'mtime'] = ent_date
				day = days_between(consent, ent_date) + 1
				

		# setting day as the difference between the consent date (day 1) and interview date
		dpdash_main.at[event_2, 'day'] = day
	
		# adding a days since consent
		dpdash_main.at[event_2, 'days_since_consent'] = days_between(consent, today)	


	#removing rows with no data
	final_csv = dpdash_main[dpdash_main.Percentage != 0]
	last_day = final_csv['day'].max()

	if name in ['lifetime_ap_exposure_screen']:
		print("Rounding ap exposure")
		final_csv[["chrap_total"]] = final_csv[["chrap_total"]].apply(pd.to_numeric)
		print(final_csv[["chrap_total"]])
		final_csv = final_csv.round({"chrap_total":1})
		print(final_csv[["chrap_total"]])

	#print("Printing final csv for: ", name)	
	#print(final_csv.T)
	
	# Saving to a csv based on ID and event
	final_csv.to_csv(output1 + site+"-"+id+"-form_"+name+'-day1to'+str(last_day)+".csv", sep=',', index = False, header=True)



#print(percentage_form_2.T["baseline_arm_1"])
percentage_form_2.to_csv(output1 + site+"-"+id+"-percentage.csv", sep=',', index = True, header=True)



