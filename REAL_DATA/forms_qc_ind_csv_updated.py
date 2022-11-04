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
#output_processed = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/processed/{1}/surveys/".format(site, id)
# creating a folder if there isn't one
#if not os.path.exists(output_processed):
#	print("Creating output directory in participant's processed directory")
	#os.makedirs(output_processed)

sub_data = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)
print("Participant json: ", sub_data)

with open(sub_data, 'r') as f:
	json = json.load(f)

sub_data_all = pd.DataFrame.from_dict(json, orient="columns")
#replacing empty cells with NaN
sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)

#print(sub_data_all.T)
# drop empty columns
sub_data_all.dropna(axis=1, how='all', inplace=True)

#print("After null column removal")
#print(sub_data_all)


# getting visit status
percpath = (output1 + site+"-"+id+"-percentage.csv")
status = "0"
if os.path.exists(percpath):
	perc_check = pd.read_csv(percpath)
	perc_check = perc_check.drop('informed_consent_run_sheet' , axis='columns')
	perc_check = perc_check[perc_check['Unnamed: 0'].str.contains('floating')==False]
	perc_check['Completed']= perc_check.iloc[:, 1:].sum(axis=1)
	perc_check['Total_empty'] = (perc_check == 0).sum(axis=1)
	print(perc_check)
	perc_check = perc_check[perc_check.Completed > 100]

	if perc_check.empty:
		status = "0"
	else:
		perc_check = perc_check.reset_index()
		status = (perc_check.index[-1] + 1)

		if perc_check['Total_empty'].min() > 58:
			status = "0"
print("Visit status: " + str(status))

#form_names = ['informed_consent_run_sheet', 'inclusionexclusion_criteria_review', 'recruitment_source', 'coenrollment_form', 'sofas_screening', 'mri_run_sheet', 'sociodemographics', 'lifetime_ap_exposure_screen']

# Subsetting data based on event
event_list = sub_data_all.redcap_event_name.unique()
screening = event_list[0]
baseline = event_list[2]
print("Screening visit: " + screening)
print("Baseline visit: " + baseline)

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
#form_names = ["guid_form"]
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
				
	
	#print("Added all the variables to the form:")
	#print(form_info_2.T)

	for event_2 in event_list:
		#print(event_2)
		form_info_2.at["Existing_variables", event_2] = 0
		form_info_2.at["Total_variables", event_2] = 0
		form_info_2.at["Missing_vars", event_2] = 0

		total = (len(form_vars) - 3)
		existing = (len(form_info_2.index) - 3)
		form_info_2.at["Existing_variables", event_2] = (len(form_info_2.index) - 3)
		form_info_2.at["Total_variables", event_2] = (len(form_vars) - 3)
		# adding the number of nas to the number of total minus existing variables
		form_info_2.at["Missing_vars", event_2] = (total - existing) + form_info_2.isna().sum()
			
		#print(form_info_2)
		# Calculating the percentage of missing as compared to the existing variables
	for event_2 in event_list:
		if form_info_2.loc["Existing_variables", event_2].values[0] > 0:
			if form_info_2.loc["Missing_vars", event_2].values[0] > 0:

				m=form_info_2.loc["Missing_vars", event_2].values[0]
				e=form_info_2.loc["Total_variables", event_2].values[0]
				form_info_2.at["Percentage", event_2] = (100 - round((m / e) * 100))

			else:
				form_info_2.at["Percentage", event_2] = 100
		else:
			form_info_2.at["Percentage", event_2] = 0

	#print("After percentage has been calculated")
	#print(form_info_2)

	# make csv with form name, percentage, event
	for event_2 in event_list:
		percentage_form_2.at[event_2, name] = form_info_2.loc["Percentage", event_2].values[0]

	#print("Printing percentage form")
	#print(percentage_form_2)

	# creating a single exclusion inclusion variable
	if name in ['inclusionexclusion_criteria_review']: 
		form_info_2.at["included_excluded", screening] = np.nan

		if "chrcrit_included" in sub_data_all and "chrcrit_included" in sub_data_all and sub_consent.loc[0, "chrcrit_included"] == "1":
			print("Participant meets inclusion criteria")
			form_info_2.at["included_excluded", screening] = 1
		
		if "chrcrit_excluded" in sub_data_all and pd.notna(sub_consent.loc[0, "chrcrit_excluded"]) and sub_consent.loc[0, "chrcrit_excluded"] == "0":
			form_info_2.at["included_excluded", screening] = 0

		print("Is this person included or not? ")				
		#print(str(form_info_2.at["included_excluded", screening]))	

	# making a yes/no GUID form for whether there is a GUID or pseudoguid
	if name in ['guid_form']: 
		if "chrguid_guid" in sub_data_all and pd.notna(sub_data_all.loc[0, "chrguid_guid"]):
			form_info_2.at["guid_available", screening] = 1
		else:
			form_info_2.at["guid_available", screening] = 0
		# pseudoguid
		if "chrguid_pseudoguid" in sub_data_all and pd.notna(sub_data_all.loc[0, "chrguid_pseudoguid"]):
			form_info_2.at["pseudoguid_available", screening] = 1
		else:
			form_info_2.at["pseudoguid_available", screening] = 0
		
	
	# adding visit status
	if name in ['informed_consent_run_sheet']: 
		form_info_2.at["visit_status", screening] = status
		print(form_info_2)

	# adding an age variable
	age = 0
	if name in ['sociodemographics']: 
		print(sub_data_all)
		if 'chrdemo_age_yrs_chr' in sub_data_all and pd.notna(sub_data_all.at[2, "chrdemo_age_yrs_chr"]):
			age = sub_data_all.at[2, "chrdemo_age_yrs_chr"]
			form_info_2.at["interview_age", baseline] = sub_data_all.at[2, "chrdemo_age_yrs_chr"]

		if 'chrdemo_age_yrs_hc' in sub_data_all and pd.notna(sub_data_all.at[2, "chrdemo_age_yrs_hc"]):
			age = sub_data_all.at[2, "chrdemo_age_yrs_hc"]
			form_info_2.at["interview_age", baseline] = sub_data_all.at[2, "chrdemo_age_yrs_hc"]
	
		print(form_info_2.T)	

		print("Age: " + str(age))
		
	form_info_2 = form_info_2.transpose()
	form_info_2.reset_index(inplace=True)

	#print("Setting up dpdash columns")
	names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
	dpdash_all = pd.DataFrame(columns = names_dash)

	frames = [dpdash_all, form_info_2]
	dpdash_main = pd.concat(frames)

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
	#final_csv = dpdash_main
	last_day = final_csv['day'].max()

	if 'lifetime_ap_exposure_screen' in final_csv:
		print("Rounding ap exposure")
		final_csv[["chrap_total"]] = final_csv[["chrap_total"]].apply(pd.to_numeric)
		print(final_csv[["chrap_total"]])
		final_csv = final_csv.round({"chrap_total":1})
		print(final_csv[["chrap_total"]])

	#print("Printing final csv for: ", name)	
	print(final_csv.T)
	
	# Saving to a csv based on ID and event
	final_csv.to_csv(output1 + site+"-"+id+"-form_"+name+'-day1to'+str(last_day)+".csv", sep=',', index = False, header=True)



print(percentage_form_2.T)
percentage_form_2.to_csv(output1 + site+"-"+id+"-percentage.csv", sep=',', index = True, header=True)



