import sys
import numpy as np
import pandas as pd
import argparse
import json
import os
from datetime import datetime

def days_between(d1, d2):
    return abs((d2 - d1).days)

id = str(sys.argv[1])
print("ID: ", id)

site = id[0:2]
print("Site: ", site)

output1 = "/data/predict/data_from_nda/formqc/"
output_processed = "/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient{0}/processed/{1}/surveys/".format(site, id)

# creating a folder if there isn't one#
#if not os.path.exists(output_processed):
#	print("Creating output directory in participant's processed directory")
#	os.makedirs(output_processed)

input_path = "/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient{0}/raw/{1}/surveys/".format(site, id)
print("Participant folder: ", input_path)

print("Loading data dictionary")
#dict = pd.read_csv('AMPSCZFormRepository_DataDictionary_2022-04-02.csv',
dict = pd.read_csv('/data/pnl/home/gj936/U24/Clinical_qc/flowqc/AMPSCZFormRepository_DataDictionary_2022-08-19_min.csv', sep= ",", index_col = False, low_memory=False)

status = "0"
percpath = (output1 + site+"-"+id+"-percentage.csv")
if os.path.exists(percpath):
	perc_check = pd.read_csv(percpath)
	perc_check = perc_check.fillna(0)
	print(perc_check)
	perc_check = perc_check.drop('informed_consent_run_sheet' , axis='columns')
	perc_check = perc_check[perc_check["Unnamed: 0"] != 99]
	perc_check['Completed']= perc_check.iloc[:, 1:].sum(axis=1)
	perc_check['Total_empty'] = (perc_check == 0).sum(axis=1)
	print(perc_check)
	perc_check = perc_check[perc_check.Completed > 300]

	if perc_check.empty:
		status = "0"
	else:
		status = perc_check["Unnamed: 0"].iloc[-1]

		if perc_check['Total_empty'].min() > 58:
			status = "0"
	print("Visit status: " + str(status))


# Getting all the form names from the data dictionary
#form_names = pd.read_csv('form_names.csv')
form_names = dict['Form Name'].unique()
percentage_form = pd.DataFrame(columns = form_names)

file = input_path+id+"_informed_consent_run_sheet.csv"
sub_consent = pd.read_csv(file)
consent = sub_consent.at[0, "chric_consent_date"]
consent = consent.split(" ")[0]

print("Participant consent date: " + consent)
consent = datetime.strptime(consent, "%d/%m/%Y")

# Subsetting data based on event
baseline_tracker = {}
screening_tracker = {}

print("\nCreating csvs")
for name in form_names:
	form_tracker = {}
	day_tracker = []
	print(name)

	if name in ["traumatic_brain_injury_screen"]: # using combined version of 3 csvs
		file = input_path+id+"_"+name+".csv.flat"	
		print("Alternative name for the TBI screen: " + file)
	else:
		file = input_path+id+"_"+name+".csv"
	#print(file)

	if os.path.isfile(file): 
		sub_data_all = pd.read_csv(file)
		#print("Printing data for form")
		#print(sub_data_all.T)
		sub_data_all = sub_data_all.replace('-3', np.NaN, regex=True)
		sub_data_all = sub_data_all.replace(-3, np.NaN, regex=True)
		print(sub_data_all['visit'].unique())
		check = 1

		for event in sub_data_all['visit'].unique():
			event = str(event)
			print("Event: " + event)
			sub_data = sub_data_all[sub_data_all['visit'].astype(str).isin([event])]
			sub_data = sub_data.reset_index(drop=True)
			#sub_data = sub_data.apply(lambda x: x.str.strip()).replace('', np.nan)

			form_info = pd.DataFrame(columns = ['Variables'])
			form = dict.loc[dict['Form Name'] == name]
			form_vars = form['Variable / Field Name'].tolist()

			# Adding all variables to the csv
			for var in form_vars:
				if var in sub_data:
					form_info.at[var, 'Variables'] = sub_data.at[0, var]

			#### setting up combined forms
			if event == '1':
				#print("Saving screening variables")
				screening_tracker["Screening_{0}".format(name)] = form_info
			
			if event == '2':
				#print("Saving baseline variables")
				baseline_tracker["Baseline_{0}".format(name)] = form_info
	
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

			form_info.at["Visit", 'Variables'] = event

			## Adding percentage data
			percentage_form.at[event, name] = form_info.at["Percentage", 'Variables']

			# adding visit status
			if name in ['informed_consent_run_sheet']: 
				print(name)
				form_info.at["visit_status", 'Variables'] = status
				print(form_info) 

			# adding included/excluded variables
			# creating a single exclusion inclusion variable
			if name in ['inclusionexclusion_criteria_review']: 
				form_info.at["included_excluded", 'Variables'] = np.nan

				if "chrcrit_included" in sub_data and sub_data.loc[0, "chrcrit_included"] == 1:
					print("Participant meets inclusion criteria")
					form_info.at["included_excluded", 'Variables'] = 1
		
				if "chrcrit_excluded" in sub_data and sub_data.at[0, "chrcrit_excluded"] == 1:
					print("Participant meets exclusion criteria")					
					form_info.at["included_excluded", 'Variables'] = 0

				print("Is this person included or not? ")				
				print(form_info.at["included_excluded", 'Variables'])

			# adding interview age
			age = 0
			if name in ['sociodemographics'] and pd.notna(sub_data.loc[0, "interview_age"]): 
				print(sub_data.T)
				form_info.at["interview_age", 'Variables'] = round((sub_data.loc[0, "interview_age"])/12)	
				print("Age: " + str(age))

			# making a yes/no GUID form for whether there is a GUID or pseudoguid
			if name in ['guid_form']: 
				if "chrguid_guid" in sub_data and pd.notna(sub_data.loc[0, "chrguid_guid"]):
					form_info.at["guid_available", 'Variables'] = 1
				else:
					form_info.at["guid_available", 'Variables'] = 0

				# pseudoguid
				if "chrguid_pseudoguid" in sub_data and pd.notna(sub_data.loc[0, "chrguid_pseudoguid"]):
					form_info.at["pseudoguid_available", 'Variables'] = 1
				else:
					form_info.at["pseudoguid_available", 'Variables'] = 0


			# creating a total score for oasis
			if name in ['oasis']:
				if sub_data.at[0, 'chroasis_oasis_1'] > -1 and sub_data.at[0, 'chroasis_oasis_2'] > -1 and sub_data.at[0, 'chroasis_oasis_3'] > -1 and sub_data.at[0, 'chroasis_oasis_4'] > -1 and sub_data.at[0, 'chroasis_oasis_5'] > -1: 					
					# creating list of values
					numbers = [sub_data.at[0, 'chroasis_oasis_1'], sub_data.at[0, 'chroasis_oasis_2'], sub_data.at[0, 'chroasis_oasis_3'], sub_data.at[0, 'chroasis_oasis_4'], sub_data.at[0, 'chroasis_oasis_5']]
					# adding values to get total score
					form_info.at["chroasis_oasis_total10", 'Variables'] = sum(numbers)				
				
				else:
					form_info.at["chroasis_oasis_total10", 'Variables'] = 999
				
	
			# transposing
			print(form_info)
			form_info = form_info.transpose()


			names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
			dpdash_main = pd.DataFrame(columns = names_dash)
			dpdash_main.at[event, 'subjectid'] = id
			dpdash_main.at[event, 'site'] = site

			if "interview_date" in sub_data:
				int_date = sub_data.at[0, "interview_date"]
				
				if pd.isna(int_date):
					dpdash_main.at[event, 'mtime'] = consent
					day = 1
				else:
					dpdash_main.at[event, 'mtime'] = int_date
					int_date = datetime.strptime(int_date, "%m/%d/%Y")
					day = days_between(consent, int_date) + 1

			else:
				if "entry_date" in sub_data:
					int_date = sub_data.at[0, "entry_date"]
					int_date = datetime.strptime(int_date, "%m/%d/%Y")
					dpdash_main.at[event, 'mtime'] = int_date
					day = days_between(consent, int_date) + 1

				else:
					#print("No interview or entry date")
					dpdash_main.at[event, 'mtime'] = consent
					day = 1

			# creating time between entry and interview date
			if "interview_date" in sub_data and "entry_date" in sub_data:
				int_date = sub_data.at[0, "interview_date"]
				ent_date = sub_data.at[0, "entry_date"]
				
				if pd.isna(int_date) | pd.isna(ent_date):
					dpdash_main.at[event, 'time_between_int_ent'] = 'NaN'
					
				else:
					ent_date = datetime.strptime(ent_date, "%m/%d/%Y")
					int_date = datetime.strptime(int_date, "%m/%d/%Y")
					dpdash_main.at[event, 'time_between_int_ent'] = days_between(ent_date, int_date)
				print("Time between interview and entry date: ")
			#print(dpdash_main.at[event, 'time_between_int_ent'])


			# setting day as the difference between the consent date (day 1) and interview date
			dpdash_main.at[event, 'day'] = day

			dpdash_main = dpdash_main.reset_index(drop=True)
			form_info = form_info.reset_index(drop=True)

			frames = [dpdash_main, form_info]
			dpdash_full = pd.concat(frames, axis=1)
			#print(dpdash_full.T)

			day_tracker.append(str(day)) 

			form_tracker["Form_event_{0}".format(event)] = dpdash_full


		#print("Length of day tracker: " + str(len(day_tracker)))
		if len(day_tracker) < 2:	
			last_day = 1
		else:
			last_day = day_tracker[-1]

		#print(last_day)
		#print(form_tracker)

		final_csv = pd.concat(form_tracker, axis=0, ignore_index=True)
		print(name)
		#removing rows with no data
		#final_csv = final_csv[final_csv.Percentage != 0]
		print("FINAL CSV TO BE EXPORTED:\n", final_csv.T)
	
		# Saving to a csv based on ID and event
		final_csv.to_csv(output1 + site+"-"+id+"-form_"+name+'-day1to'+str(last_day)+".csv", sep=',', index = False, header=True)


else:
	print("No file for form: " + name)


### putting together the combined csvs	
print("Printing trackers")

if len(screening_tracker) > 0:
	screening = pd.concat(screening_tracker, axis=0)
	print(screening)
	screening.to_csv(output1 + site+"-"+id+"-screening.csv", sep=',', index = True, header=True)

if len(baseline_tracker) > 0:
	baseline = pd.concat(baseline_tracker, axis=0)
	print(baseline)
	baseline.to_csv(output1 + site+"-"+id+"-baseline.csv", sep=',', index = True, header=True)



print(percentage_form.T)
percentage_form.to_csv(output1 + site+"-"+id+"-percentage.csv", sep=',', index = True, header=True)



