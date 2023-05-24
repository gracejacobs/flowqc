import sys
import numpy as np
import pandas as pd
import argparse
import json
import os
import math
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

network = str(sys.argv[2])
print("Network: ", network)

#output1 = "/data/predict/data_from_nda/formqc/"
output1 = "/data/predict1/data_from_nda/formqc_test/"

if network == "PRONET":
	sub_data = "/data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)
	print("Participant json: ", sub_data)
else:
	sub_data = "/data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient{0}/raw/{1}/surveys/{1}.Prescient.json".format(site, id)
	print("Participant json: ", sub_data)


with open(sub_data, 'r') as f:
	json = json.load(f)

sub_data_all = pd.DataFrame.from_dict(json, orient="columns")
#replacing empty cells with NaN
sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)

#print(sub_data_all.T)
# drop empty columns
sub_data_all.dropna(axis=1, how='all', inplace=True)

print("Printing raw data")
print(sub_data_all)

# getting visit status
percpath = (output1 + site+"-"+id+"-percentage.csv")
status = "0"
if os.path.exists(percpath):
	perc_check = pd.read_csv(percpath)
	print(perc_check)
	perc_check = perc_check.drop('informed_consent_run_sheet' , axis='columns')
	perc_check = perc_check[perc_check['Index'].str.contains('floating')==False]
	perc_check['Completed']= perc_check.iloc[:, 1:].sum(axis=1)
	perc_check['Total_empty'] = (perc_check == 0).sum(axis=1)
	print("All percentages")
	#pd.set_option('display.max_rows', None)
	print(perc_check)
	#print(perc_check.transpose())
	perc_check_2 = perc_check[perc_check.Completed > 100]
	# adding conversion possibility
	perc_check = perc_check[perc_check['Index'].str.contains('convers')==True]
	perc_check = perc_check.reset_index()
	#print(perc_check)

	if perc_check_2.empty:
		status = "0"
	else:
		perc_check_2 = perc_check_2.reset_index()
		status = (perc_check_2.index[-1] + 1)

		if perc_check_2['Total_empty'].min() > 58:
			status = "0"
	if not perc_check.empty:		
		if perc_check.loc[0, 'Completed'] > 20:
			status = "98" #conversion

print("VISIT STATUS: " + str(status))

#form_names = ['pubertal_developmental_scale', 'informed_consent_run_sheet', 'inclusionexclusion_criteria_review', 'recruitment_source', 'coenrollment_form', 'sofas_screening', 'mri_run_sheet', 'sociodemographics', 'lifetime_ap_exposure_screen']

# Subsetting data based on event
event_list = sub_data_all.redcap_event_name.unique()
print("Event List: ", event_list)
screening = [i for i in event_list if i.startswith('screening_')][0]

baseline = [i for i in event_list if i.startswith('baseline_')]
if baseline != []:
	baseline = baseline[0]

month2 = [i for i in event_list if i.startswith('month_2')]
if month2 != []:
	month2 = month2[0]

month1 = [i for i in event_list if i.startswith('month_1')]
if month1 != []:
	month1 = month1[0]

month3 = [i for i in event_list if i.startswith('month_3')]
if month3 != []:
	month3 = month3[0]

month4 = [i for i in event_list if i.startswith('month_4')]
if month4 != []:
	month4 = month4[0]

month5 = [i for i in event_list if i.startswith('month_5')]
if month5 != []:
	month5 = month5[0]

month6 = [i for i in event_list if i.startswith('month_6')]
if month6 != []:
	month6 = month6[0]

month7 = [i for i in event_list if i.startswith('month_7')]
if month7 != []:
	month7 = month7[0]

month8 = [i for i in event_list if i.startswith('month_8')]
if month8 != []:
	month8 = month8[0]

month9 = [i for i in event_list if i.startswith('month_9')]
if month9 != []:
	month9 = month9[0]


conversion = [i for i in event_list if i.startswith('conversion_')]
if conversion != []:
	conversion = conversion[0]


# Opening data dictionary
dict = pd.read_csv('/data/pnl/home/gj936/U24/Clinical_qc/flowqc/AMPSCZFormRepository_DataDictionary_2022-08-19_min.csv', sep= ",", index_col = False, low_memory=True)

# Getting all the form names from the data dictionary
form_names = dict['Form Name'].unique()
#form_names = ['sociodemographics', 'pubertal_developmental_scale', 'perceived_discrimination_scale', 'informed_consent_run_sheet', 'inclusionexclusion_criteria_review', 'recruitment_source', 'coenrollment_form', 'sofas_screening', 'mri_run_sheet',  'lifetime_ap_exposure_screen']

form_names = np.append(form_names, "chart_statuses")
#print(form_names)

percentage_form_2 = pd.DataFrame(columns = form_names)
#print(percentage_form)

sub_consent = sub_data_all[sub_data_all['redcap_event_name'].isin([screening])]
consent = sub_consent.at[0, "chric_consent_date"]
if id == "IR03960":
	print("CHECK CONSENT DATE HAS BEEN CORRECTED: " + consent)
	consent = sub_consent.at[0, "enrollmentnote_dateofconsent"]
print("Consent date:" + consent)

sub_data_screening = sub_data_all[sub_data_all['redcap_event_name'].isin([screening])]
sub_data_screening = sub_data_screening.reset_index(drop=True)
if baseline != []:
	sub_data_baseline = sub_data_all[sub_data_all['redcap_event_name'].isin([baseline])]
	sub_data_baseline = sub_data_baseline.reset_index(drop=True)
else:
	sub_data_baseline = pd.DataFrame()

if month2 != []:
	sub_data_month2 = sub_data_all[sub_data_all['redcap_event_name'].isin([month2])]
	sub_data_month2 = sub_data_month2.reset_index(drop=True)
else:
	sub_data_month2 = pd.DataFrame()


oasis_bl = 0
cdss_bl = 0
promis_bl = 0
cssrs_bl = 0
nsipr_bl = 0
bprs_bl = 0

oasis_m2 = 0
cdss_m2 = 0
promis_m2 = 0
bprs_m2 = 0
nsipr_m2 = 0
cssrs_m2 = 0

included = np.nan
cognition_status = 0
blood_status = 0
saliva_status = 0
clinical_status = 0
date_drawn_bl = np.nan
date_ate_bl = np.nan
date_drawn_m2 = np.nan
date_ate_m2 = np.nan

print(id)
#form_names = ["guid_form"]
#Looping through forms to get form data
for name in form_names:
	form_tracker = {}
	day_tracker = []

	form = dict.loc[dict['Form Name'] == name]
	form_vars = form['Variable / Field Name'].tolist()

	#print(id)
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

#	print("Printing percentage form")
#	print(percentage_form_2)
	if name in ['chart_statuses']: 
		form_info_2.at["Percentage", screening] = 100 #so that I don't axe it at the end
		form_info_2.at["included_excluded", screening] = included
		form_info_2.at["cognition_status", screening] = cognition_status
		form_info_2.at["blood_status", screening] = blood_status
		form_info_2.at["saliva_status", screening] = saliva_status
		form_info_2.at["clinical_status", screening] = clinical_status
		print("Calculating time fasting: ")
		if pd.isna(date_drawn_bl) or pd.isna(date_ate_bl):
			print("Basline not complete")

		else:
			print(str(date_drawn_bl))
			print(str(date_ate_bl))
			print(str((((date_drawn_bl - date_ate_bl).total_seconds()) / 60) / 60))
			form_info_2.at["time_fasting", baseline] = (((date_drawn_bl - date_ate_bl).total_seconds()) / 60) / 60
		if pd.isna(date_drawn_m2) or pd.isna(date_ate_m2):
			print("Month 2 not complete")
		else:
			print(str(date_drawn_m2))
			print(str(date_ate_m2))
			form_info_2.at["time_fasting", month2] = (((date_drawn_m2 - date_ate_m2).total_seconds()) / 60) /60
		#print(form_info_2)


	# creating a single exclusion inclusion variable
	if name in ['inclusionexclusion_criteria_review']: 
		form_info_2.at["Percentage", screening] = 100 
		if "chrcrit_included" in sub_data_all and sub_consent.loc[0, "chrcrit_included"] == "1":
			print("INCLUDED")
			included = 1
			form_info_2.at["included_excluded", screening] = 1

		if "chrcrit_included" in sub_data_all and sub_consent.loc[0, "chrcrit_included"] == "0":
			print("EXCLUDED")
			included = 0
			form_info_2.at["included_excluded", screening] = 0

		#if "chrcrit_included" in sub_data_all and pd.notna(sub_consent.loc[0, "chrcrit_included"]):
		#	form_info_2.at["included_excluded", screening] = np.nan
			

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
		form_info_2.loc["Percentage" , :] = 0
		form_info_2.at["Percentage", screening] = 100
		print(form_info_2)

	# adding an age variable
	age = 0
	age_months = 0
	if name in ['sociodemographics']: 
		#print(sub_data_all)

		# getting the time difference between consent date and the date of the sociodemographics form
		# need months to be more precise
		if 'chrdemo_age_mos_chr' in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrdemo_age_mos_chr"]):
			age_months = sub_data_baseline.at[0, "chrdemo_age_mos_chr"]

		if 'chrdemo_age_mos_hc' in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrdemo_age_mos_hc"]):
			age_months = sub_data_baseline.at[0, "chrdemo_age_mos_hc"]

		
		if 'chrdemo_entry_date' in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrdemo_entry_date"]):
			int_date = sub_data_baseline.at[0, "chrdemo_entry_date"]
			months_bet = (days_between(consent, int_date))/30

			age_months = int(age_months)
			age = round(age_months/12, 1)

			age_adj = age_months - months_bet
			age_adj = round(age_adj/12, 1)

			print("Adjusted age: "  + str(age_adj))
			form_info_2.at["consent_age", baseline] = age_adj
			form_info_2.at["interview_age", baseline] = age
		
		print("Age: " + str(age))

	# adding IQ measure or not - if past baseline, marked
	# get at baseline, bl + m2, M2 no bl, not completed	
	if name in ['iq_assessment_wasiii_wiscv_waisiv']: 
		if "chriq_fsiq" in sub_data_baseline and "chrpenn_complete" in sub_data_baseline and "chrpreiq_standard_score" in sub_data_baseline:
			bl_cog = 0
			m2_cog = 0	
			
			if pd.notna(sub_data_baseline.at[0, "chriq_fsiq"]) and pd.notna(sub_data_baseline.at[0, "chrpreiq_standard_score"]) and pd.notna(sub_data_baseline.at[0, "chrpenn_complete"]) and sub_data_baseline.at[0, "chrpenn_complete"] != "3":
				print("Baseline cog complete")
				bl_cog = 1
			if(sub_data_month2.empty == False):
				print("Month 2 (4) is in index")
				if pd.notna(sub_data_month2.at[0, "chrpenn_complete"]) and sub_data_month2.at[0, "chrpenn_complete"] != "3":
					print("Month2 cog complete")
					m2_cog = 1
			
			
			if bl_cog == 1 and m2_cog == 1:
				print("Both baseline and month 2 cog")
				cognition_status = 3
				form_info_2.at["cognition_status", screening] = 3
			if bl_cog == 1 and m2_cog == 0:
				print("Only baseline cog")
				cognition_status = 1
				form_info_2.at["cognition_status", screening] = 1
			if bl_cog == 0 and m2_cog == 1:
				print("ONly month 2 cog")
				cognition_status = 2
				form_info_2.at["cognition_status", screening] = 2

		else: 
			print("No cognition completed")
			cognition_status = 0
			form_info_2.at["cognition_status", screening] = 0
			
				
		

		# if value is there and they are past baseline they have it
		if "chriq_fsiq" in sub_data_all and pd.notna(sub_data_baseline.at[0, "chriq_fsiq"]) and int(status) > 2: 
			wasi_iq = 1
		else:
			wasi_iq = 0
		if int(status) < 3: 
			wasi_iq = 2

		form_info_2.at["wasi_iq", baseline] = wasi_iq

		print("WASI IQ Label: " + str(wasi_iq))

	# adding WRAT
	if name in ['premorbid_iq_reading_accuracy']:

	# if value is there and they are past baseline they have it
		if "chrpreiq_total_raw" in sub_data_all and pd.notna(sub_data_baseline.at[0, "chrpreiq_total_raw"]) and int(status) > 2: 
			wrat_iq = 1
		else:
			wrat_iq = 0
		if int(status) < 3: 
			wrat_iq = 2

		form_info_2.at["wrat_iq", baseline] = wrat_iq
		print("Status: " + str(status))

		print("WRAT Label: " + str(wrat_iq) + "\n")

	
	## adding saliva
	if name in ['daily_activity_and_saliva_sample_collection']:
		s_1_m2 = 0
		s_2_m2 = 0
		s_3_m2 = 0
		s_4_m2 = 0
		s_5_m2 = 0
		s_6_m2 = 0
		if "chrsaliva_vol1a" in sub_data_all:
			s_1 = sub_data_baseline.at[0, "chrsaliva_vol1a"]
			if(sub_data_month2.empty == False):
				s_1_m2 = sub_data_month2.at[0, "chrsaliva_vol1a"]
		else: 
			s_1 = 0
		if "chrsaliva_vol1b" in sub_data_all:
			s_2 = sub_data_baseline.at[0, "chrsaliva_vol1b"]
			if sub_data_month2.empty == False and 'chrsaliva_vol1b' in sub_data_month2:
				s_2_m2 = sub_data_month2.at[0, "chrsaliva_vol1b"]
		else: 
			s_2 = 0

		if "chrsaliva_vol2a" in sub_data_all:
			s_3 = str(sub_data_baseline.at[0, "chrsaliva_vol2a"])
			if sub_data_month2.empty == False and 'chrsaliva_vol2a' in sub_data_month2:

				print("Check")
				s_3_m2 = sub_data_month2.at[0, "chrsaliva_vol2a"]
		else: 
			s_3 = 0

		if "chrsaliva_vol2b" in sub_data_all:
			s_4 = sub_data_baseline.at[0, "chrsaliva_vol2b"]
			if(sub_data_month2.empty == False and 'chrsaliva_vol2b' in sub_data_month2):
				s_4_m2 = sub_data_month2.at[0, "chrsaliva_vol2b"]
		else: 
			s_4 = 0
		if "chrsaliva_vol3a" in sub_data_all:	
			s_5 = sub_data_baseline.at[0, "chrsaliva_vol3a"]
			if(sub_data_month2.empty == False):
				s_5_m2 = sub_data_month2.at[0, "chrsaliva_vol3a"]
		else: 
			s_5 = 0

		if "chrsaliva_vol3b" in sub_data_all:
			s_6 = sub_data_baseline.at[0, "chrsaliva_vol3b"]
			if(sub_data_month2.empty == False):
				s_6_m2 = sub_data_month2.at[0, "chrsaliva_vol3b"]
		else: 
			s_6 = 0

		vials = [s_1, s_2, s_3, s_4, s_5, s_6]
		vials_m2 = [s_1_m2, s_2_m2, s_3_m2, s_4_m2, s_5_m2, s_6_m2]
		print(vials)		
		print(vials_m2)

		for x in range(len(vials)):
			if pd.isnull(vials[x]) or vials[x] == '-9' or vials[x] == '-3' or vials[x] == 'nan':
				vials[x] = 0

		for x in range(len(vials_m2)):
			if pd.isnull(vials_m2[x]) or vials_m2[x] == '-9' or vials_m2[x] == '-3' or vials_m2[x] == 'nan':
				vials_m2[x] = 0

		print(vials)		
		print(vials_m2)
		#vials_m2[np.isnan(vials_m2)] = 0
		print("Printing VIAL VOLUMES")
		for i in range(len(vials)):
			if isinstance(vials[i], str):
				vials[i] = eval(vials[i])

		for i in range(len(vials_m2)):
			if isinstance(vials_m2[i], str):
				vials_m2[i] = eval(vials_m2[i])
		print(vials)
		print(vials_m2)
		print(str(np.count_nonzero(vials)))
		print(str(np.count_nonzero(vials_m2)))

		if np.count_nonzero(vials) > 4 and np.count_nonzero(vials_m2) > 4:
			print("Both complete")
			saliva_status = 3
			form_info_2.at["saliva_status", screening] = 3
		if np.count_nonzero(vials) > 4 and np.count_nonzero(vials_m2) < 5:
			print("Just baseline")
			saliva_status = 1
			form_info_2.at["saliva_status", screening] = 1
		if np.count_nonzero(vials) < 5 and np.count_nonzero(vials_m2) > 4:
			print("Just month 2")
			saliva_status = 2
			form_info_2.at["saliva_status", screening] = 2
		if np.count_nonzero(vials) < 5 and np.count_nonzero(vials_m2) < 5:
			print("Incomplete")
			saliva_status = 0
			form_info_2.at["saliva_status", screening] = 0
			

	## adding blood biomarkers
	if name in ['blood_sample_preanalytic_quality_assurance']: 
		if "chrblood_drawdate" in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrblood_drawdate"]) and sub_data_baseline.at[0, "chrblood_drawdate"] != -3 and sub_data_baseline.at[0, "chrblood_drawdate"] != -9:
			date_drawn_bl = sub_data_baseline.at[0, "chrblood_drawdate"]
			date_drawn_bl = datetime.strptime(date_drawn_bl, "%Y-%m-%d %H:%M")
			print(str(date_drawn_bl))
		if "chrblood_drawdate" in sub_data_month2 and pd.notna(sub_data_month2.at[0, "chrblood_drawdate"]) and sub_data_month2.at[0, "chrblood_drawdate"] != -3 and sub_data_month2.at[0, "chrblood_drawdate"] != -9:
			date_drawn_m2 = sub_data_month2.at[0, "chrblood_drawdate"]
			date_drawn_m2 = datetime.strptime(date_drawn_m2, "%Y-%m-%d %H:%M")
			print(str(date_drawn_m2))

		if "chrblood_plasma_freeze" in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrblood_plasma_freeze"]):
			plasma_time = sub_data_baseline.at[0, "chrblood_plasma_freeze"]
			print(str(plasma_time))
			plasma_time = int(plasma_time)

			plasma = 0

			if plasma_time < 60 and plasma_time > 0:
				plasma = 1
			if plasma_time > 60 and plasma_time < 121:
				plasma = 2
			if plasma_time > 120:
				plasma = 3

			print ("Plasma processing time: " + str(plasma_time) + " Label: " + str(plasma) + "\n")
			form_info_2.at["plasma_time", baseline] = plasma

		if "chrblood_buffy_freeze" in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrblood_buffy_freeze"]):
			buffy_time = sub_data_baseline.at[0, "chrblood_buffy_freeze"]
			print(str(buffy_time))
			buffy_time = int(buffy_time)

			buffy = 0

			if buffy_time < 60 and buffy_time > 0:
				buffy = 1
			if buffy_time > 60 and buffy_time < 121:
				buffy = 2
			if buffy_time > 120:
				buffy = 3

			print ("Buffy processing time: " + str(buffy_time) + " Label: " + str(buffy) + "\n")
			form_info_2.at["buffy_time", baseline] = buffy
		
			# EDTA plasma sample
		wb_1_m2 = 0
		wb_2_m2 = 0
		wb_3_m2 = 0
		sr_1_m2 = 0
		sr_2_m2 = 0
		sr_3_m2 = 0
		pl_1_m2 = 0
		pl_2_m2 = 0
		pl_3_m2 = 0
		pl_4_m2 = 0
		pl_5_m2 = 0
		pl_6_m2 = 0
		bc_1_m2 = 0


		if "chrblood_bc1vol" in sub_data_all:
			bc_1 = sub_data_baseline.at[0, "chrblood_bc1vol"]
			if "chrblood_bc1vol" in sub_data_month2:
				bc_1_m2 = sub_data_month2.at[0, "chrblood_bc1vol"]
		else: 
			bc_1 = 0
		if "chrblood_wb1vol" in sub_data_all:
			wb_1 = sub_data_baseline.at[0, "chrblood_wb1vol"]
			if "chrblood_wb1vol" in sub_data_month2:
				wb_1_m2 = sub_data_month2.at[0, "chrblood_wb1vol"]
		else: 
			wb_1 = 0
		if "chrblood_wb2vol" in sub_data_all:
			wb_2 = sub_data_baseline.at[0, "chrblood_wb2vol"]
			if "chrblood_wb2vol" in sub_data_month2:
				wb_2_m2 = sub_data_month2.at[0, "chrblood_wb2vol"]
		else: 
			wb_2 = 0
		if "chrblood_wb3vol" in sub_data_all:	
			wb_3 = sub_data_baseline.at[0, "chrblood_wb3vol"]
			if "chrblood_bc1vol" in sub_data_month2:
				wb_3_m2 = sub_data_month2.at[0, "chrblood_wb3vol"]
		else: 
			wb_3 = 0

		if "chrblood_se1vol" in sub_data_all:
			sr_1 = sub_data_baseline.at[0, "chrblood_se1vol"]
			if "chrblood_se1vol" in sub_data_month2:
				sr_1_m2 = sub_data_month2.at[0, "chrblood_se1vol"]
		else: 
			sr_1 = 0
		if "chrblood_se2vol" in sub_data_all:
			sr_2 = sub_data_baseline.at[0, "chrblood_se2vol"]
			if "chrblood_se2vol" in sub_data_month2:
				sr_2_m2 = sub_data_month2.at[0, "chrblood_se2vol"]
		else: 
			sr_2 = 0
		if "chrblood_se3vol" in sub_data_all:
			sr_3 = sub_data_baseline.at[0, "chrblood_se3vol"]
			if "chrblood_se3vol" in sub_data_month2:
				sr_3_m2 = sub_data_month2.at[0, "chrblood_se3vol"]
		else: 
			sr_3 = 0

		if "chrblood_pl1vol" in sub_data_all:
			pl_1 = sub_data_baseline.at[0, "chrblood_pl1vol"]
			if "chrblood_pl1vol" in sub_data_month2:
				pl_1_m2 = sub_data_month2.at[0, "chrblood_pl1vol"]
		else: 
			pl_1 = 0
		if "chrblood_pl2vol" in sub_data_all:
			pl_2 = sub_data_baseline.at[0, "chrblood_pl2vol"]
			if "chrblood_pl2vol" in sub_data_month2:
				pl_2_m2 = sub_data_month2.at[0, "chrblood_pl2vol"]
		else: 
			pl_2 = 0
		if "chrblood_pl3vol" in sub_data_all:
			pl_3 = sub_data_baseline.at[0, "chrblood_pl3vol"]
			if "chrblood_pl3vol" in sub_data_month2:
				pl_3_m2 = sub_data_month2.at[0, "chrblood_pl3vol"]
		else: 
			pl_3 = 0
		if "chrblood_pl4vol" in sub_data_all:
			pl_4 = sub_data_baseline.at[0, "chrblood_pl4vol"]
			if "chrblood_pl4vol" in sub_data_month2:
				pl_4_m2 = sub_data_month2.at[0, "chrblood_pl4vol"]
		else: 
			pl_4 = 0
		if "chrblood_pl5vol" in sub_data_all:
			pl_5 = sub_data_baseline.at[0, "chrblood_pl5vol"]
			if "chrblood_pl5vol" in sub_data_month2:
				pl_5_m2 = sub_data_month2.at[0, "chrblood_pl5vol"]
		else: 
			pl_5 = 0
		if "chrblood_pl6vol" in sub_data_all:
			pl_6 = sub_data_baseline.at[0, "chrblood_pl6vol"]
			if "chrblood_pl6vol" in sub_data_month2:
				pl_6_m2 = sub_data_month2.at[0, "chrblood_pl6vol"]
		else: 
			pl_6 = 0

		vials = [wb_1, wb_2, wb_3, sr_1, sr_2, sr_3, pl_1, pl_2, pl_3, pl_4, pl_5, pl_6, bc_1]
		vials_m2 = [wb_1_m2, wb_2_m2, wb_3_m2, sr_1_m2, sr_2_m2, sr_3_m2, pl_1_m2, pl_2_m2, pl_3_m2, pl_4_m2, pl_5_m2, pl_6_m2, bc_1_m2]
		print(vials)
		print(vials_m2)

		for x in range(len(vials)):
			if pd.isnull(vials[x]) or vials[x] == '-9' or vials[x] == '-3' or vials[x] == 'nan':
				vials[x] = 0

		for x in range(len(vials_m2)):
			if pd.isnull(vials_m2[x]) or vials_m2[x] == '-9' or vials_m2[x] == '-3' or vials_m2[x] == 'nan':
				vials_m2[x] = 0

		#vials_m2[np.isnan(vials_m2)] = 0
		print("Printing VIAL VOLUMES")
		#if isinstance(vials[1], str):
		#	vials = [eval(i) for i in vials]
		#if isinstance(vials_m2[1], str):
		#	vials_m2 = [eval(i) for i in vials_m2]
		for i in range(len(vials)):
			if isinstance(vials[i], str):
				vials[i] = eval(vials[i])

		for i in range(len(vials_m2)):
			if isinstance(vials_m2[i], str):
				vials_m2[i] = eval(vials_m2[i])
		print(vials)
		print(vials_m2)
		print(str(np.count_nonzero(vials)))
		print(str(np.count_nonzero(vials_m2)))

		if np.count_nonzero(vials) > 9 and np.count_nonzero(vials_m2) > 9:
			print("Both complete")
			blood_status = 3
			form_info_2.at["blood_status", screening] = 3
		if np.count_nonzero(vials) > 9 and np.count_nonzero(vials_m2) < 10:
			print("Just baseline")
			blood_status = 1
			form_info_2.at["blood_status", screening] = 1
		if np.count_nonzero(vials) < 10 and np.count_nonzero(vials_m2) > 9:
			print("Just month 2")
			blood_status = 2
			form_info_2.at["blood_status", screening] = 2
		if np.count_nonzero(vials) < 10 and np.count_nonzero(vials_m2) < 10:
			print("Incomplete")
			blood_status = 0
			form_info_2.at["blood_status", screening] = 0

	if name in ['current_health_status']: 
		if "chrchs_ate" in sub_data_all and pd.notna(sub_data_baseline.at[0, "chrchs_ate"]) and sub_data_baseline.at[0, "chrchs_ate"] != '-3' and sub_data_baseline.at[0, "chrchs_ate"] != '-9':
		#with pd.option_context('display.max_rows', None, 'display.precision', 3,):
			#print(form_info_2)
			date_ate_bl = sub_data_baseline.at[0, "chrchs_ate"]
			date_ate_bl = datetime.strptime(date_ate_bl, "%Y-%m-%d %H:%M")
			print(str(date_ate_bl))
		
		if not sub_data_month2.empty:
			if "chrchs_ate" in sub_data_month2 and pd.notna(sub_data_month2.at[0, "chrchs_ate"]) and sub_data_month2.at[0, "chrchs_ate"] != '-3' and sub_data_month2.at[0, "chrchs_ate"] != '-9':
				date_ate_m2 = sub_data_month2.at[0, "chrchs_ate"]
				date_ate_m2 = datetime.strptime(date_ate_m2, "%Y-%m-%d %H:%M")
				print(str(date_ate_m2))
			
	#if name in ['daily_activity_and_saliva_sample_collection']:
		#with pd.option_context('display.max_rows', None, 'display.precision', 3,):
			#print(form_info_2)

	############ Missing variables for clinical data
	if name in ['perceived_discrimination_scale'] and 2 in sub_data_all.index: 
		# baseline 
		if "chrdim_dim_yesno_q1_1" not in sub_data_all or "chrdlm_dim_yesno_q1_2" not in sub_data_all or "chrdlm_dim_sex" not in sub_data_all or "chrdlm_dim_yesno_age" not in sub_data_all or "chrdlm_dim_yesno_q4_1" not in sub_data_all or "chrdlm_dim_yesno_q5" not in sub_data_all or "chrdlm_dim_yesno_q3" not in sub_data_all or "chrdlm_dim_yesno_q6" not in sub_data_all or "chrdlm_dim_yesno_other" not in sub_data_all:
			#value = 0
			#print("Missing variables in json: " + value)
			form_info_2.at["no_missing_pdiscrims", baseline] = 0 # missing data
	
		else: # still could be missing data - check if any are null
			print("Not missing variables in json")
			form_info_2.at["no_missing_pdiscrims", baseline] = 0

			if pd.isnull(sub_data_baseline.at[0, "chrdim_dim_yesno_q1_1"]) or pd.isnull(sub_data_baseline.at[0, "chrdlm_dim_yesno_q1_2"]) or pd.isnull(sub_data_all.at[2, "chrdlm_dim_sex"]) or pd.isnull(sub_data_all.at[2, "chrdlm_dim_yesno_age"]) or pd.isnull(sub_data_all.at[2, "chrdlm_dim_yesno_q4_1"]) or pd.isnull(sub_data_all.at[2, "chrdlm_dim_yesno_q5"]) or pd.isnull(sub_data_all.at[2, "chrdlm_dim_yesno_q3"]) or pd.isnull(sub_data_all.at[2, "chrdlm_dim_yesno_q6"]) or pd.isnull(sub_data_all.at[2, "chrdlm_dim_yesno_other"]):			
				print("One of the variables is missing")
				check = 0
				print(str(check))
				form_info_2.at["no_missing_pdiscrims", baseline] = check
			else: 
				print("None of the variables are missing")
				form_info_2.at["no_missing_pdiscrims", baseline] = "1"

		##print(form_info_2.at["no_missing_pdiscrims", baseline].to_string)

	if name in ['pubertal_developmental_scale'] and 2 in sub_data_all.index: 
		# baseline
		pub_first_q = 0
		pub_m_q = 0
		pub_f_q = 0
		print("First check if variables exist")
		if "chrpds_pds_1_p" in sub_data_all and "chrpds_pds_1_p" in sub_data_all and "chrpds_pds_2_p" in sub_data_all and "chrpds_pds_3_p" in sub_data_all:
			print("No missing variables")
			pub_first_q = 0
			if pd.isnull(sub_data_all.at[2, "chrpds_pds_1_p"]) or pd.isnull(sub_data_all.at[2, "chrpds_pds_2_p"]) or  pd.isnull(sub_data_all.at[2, "chrpds_pds_3_p"]):
				print("One is null")
				pub_first_q = 0 # missing data
			else:
				print("No missing variables")
				pub_first_q = 1 # has all data

			# male questions
			if "chrdemo_sexassigned" in sub_data_all and sub_data_all.at[2, "chrdemo_sexassigned"] == "1" and "chrpds_pds_1_p" in sub_data_all and "chrpds_pds_m5_p" in sub_data_all:
				if pd.isnull(sub_data_all.at[2, "chrpds_pds_m4_p"]) or pd.isnull(sub_data_all.at[2, "chrpds_pds_m5_p"]):
					print("Missing male variables")
					pub_m_q = 0 # missiing data
				else:
					print("No missing male variables")
					pub_m_q = 1 # looks like has data	
			# female questions
			if "chrdemo_sexassigned" in sub_data_all and sub_data_all.at[2, "chrdemo_sexassigned"] == "2" and "chrpds_pds_f4_p" in sub_data_all and "chrpds_pds_f5b_p" in sub_data_all:
				if pd.isnull(sub_data_all.at[2, "chrpds_pds_f4_p"]) or pd.isnull(sub_data_all.at[2, "chrpds_pds_f5b_p"]):
					print("Missing female variables")
					pub_f_q = 0 #missing data
				else:
					pub_f_q = 1 #not missing data
			# all questions
			if pub_first_q == 1 and pub_m_q == 1 or pub_f_q == 1:
				print("Not missing variables")
				form_info_2.at["no_missing_pubds", baseline] = "1"
			else: 
				form_info_2.at["no_missing_pubds", baseline] = "0"
 
	if name in ['nsipr'] and 2 in sub_data_all.index: 
		# baseline
		if "chrnsipr_item1_rating" not in sub_data_all or "chrnsipr_item2_rating" not in sub_data_all or "chrnsipr_item3_rating" not in sub_data_all or "chrnsipr_item4_rating" not in sub_data_all or "chrnsipr_item5_rating" not in sub_data_all or "chrnsipr_item6_rating" not in sub_data_all or "chrnsipr_item7_rating" not in sub_data_all or "chrnsipr_item8_rating" not in sub_data_all or "chrnsipr_item9_rating" not in sub_data_all or "chrnsipr_item10_rating" not in sub_data_all or "chrnsipr_item11_rating" not in sub_data_all:
			print("Missing variables")
			form_info_2.at["no_missing_nsipr", baseline] = "0" # missing data
			form_info_2.at["no_missing_nsipr", month2] = "0" 
		else:
			if pd.notna(sub_data_all.at[2, "chrnsipr_item1_rating"]) and pd.notna(sub_data_all.at[2, "chrnsipr_item2_rating"]) and pd.notna(sub_data_all.at[2, "chrnsipr_item3_rating"]) and pd.notna(sub_data_all.at[2, "chrnsipr_item4_rating"]) and pd.notna(sub_data_all.at[2, "chrnsipr_item5_rating"]) and pd.notna(sub_data_all.at[2, "chrnsipr_item6_rating"]) and pd.notna(sub_data_all.at[2, "chrnsipr_item7_rating"]) and pd.notna(sub_data_all.at[2, "chrnsipr_item8_rating"]) and pd.notna(sub_data_all.at[2, "chrnsipr_item9_rating"]) and pd.notna(sub_data_all.at[2, "chrnsipr_item10_rating"]) and pd.notna(sub_data_all.at[2, "chrnsipr_item11_rating"]):
				print("yay no missing")
				nsipr_bl = 1
				form_info_2.at["no_missing_nsipr", baseline] = "1" # none are NA
			else: 
				print("One item is missing")
				form_info_2.at["no_missing_nsipr", baseline] = "0"

			if 4 in sub_data_all.index:
				if pd.notna(sub_data_all.at[4, "chrnsipr_item1_rating"]) and pd.notna(sub_data_all.at[4, "chrnsipr_item2_rating"]) and pd.notna(sub_data_all.at[4, "chrnsipr_item3_rating"]) and pd.notna(sub_data_all.at[4, "chrnsipr_item4_rating"]) and pd.notna(sub_data_all.at[4, "chrnsipr_item5_rating"]) and pd.notna(sub_data_all.at[4, "chrnsipr_item6_rating"]) and pd.notna(sub_data_all.at[4, "chrnsipr_item7_rating"]) and pd.notna(sub_data_all.at[4, "chrnsipr_item8_rating"]) and pd.notna(sub_data_all.at[4, "chrnsipr_item9_rating"]) and pd.notna(sub_data_all.at[4, "chrnsipr_item10_rating"]) and pd.notna(sub_data_all.at[4, "chrnsipr_item11_rating"]):
					print("yay no missing in month2")
					nsipr_m2 = 1
					form_info_2.at["no_missing_nsipr", month2] = "1" # none are NA
				else: 
					form_info_2.at["no_missing_nsipr", month2] = "0"
			else:
				form_info_2.at["no_missing_nsipr", month2] = "0"

	if name in ['cdss'] and 2 in sub_data_all.index: 
		# baseline
		if "chrcdss_calg1" not in sub_data_all or  "chrcdss_calg2" not in sub_data_all or "chrcdss_calg3" not in sub_data_all or "chrcdss_calg4" not in sub_data_all or "chrcdss_calg5" not in sub_data_all or "chrcdss_calg6" not in sub_data_all or "chrcdss_calg7" not in sub_data_all or "chrcdss_calg8" not in sub_data_all or "chrcdss_calg9" not in sub_data_all:
			print("Missing variables")
			form_info_2.at["no_missing_cdss", baseline] = "0" #missing data
			form_info_2.at["no_missing_cdss", month2] = "0"
		else:
			if pd.notna(sub_data_all.at[2, "chrcdss_calg1"]) and pd.notna(sub_data_all.at[2, "chrcdss_calg2"]) and pd.notna(sub_data_all.at[2, "chrcdss_calg3"]) and pd.notna(sub_data_all.at[2, "chrcdss_calg4"]) and pd.notna(sub_data_all.at[2, "chrcdss_calg5"]) and pd.notna(sub_data_all.at[2, "chrcdss_calg6"]) and pd.notna(sub_data_all.at[2, "chrcdss_calg7"]) and pd.notna(sub_data_all.at[2, "chrcdss_calg8"]) and pd.notna(sub_data_all.at[2, "chrcdss_calg9"]):
					
				print("No missin data yay")
				cdss_bl = 1
				form_info_2.at["no_missing_cdss", baseline] = "1" #none are nan so not missing
			else: 
				print("Missing variables")
				form_info_2.at["no_missing_cdss", baseline] = "0"
			if 4 in sub_data_all:
				if pd.notna(sub_data_all.at[4, "chrcdss_calg1"]) and pd.notna(sub_data_all.at[4, "chrcdss_calg2"]) and pd.notna(sub_data_all.at[4, "chrcdss_calg3"]) and pd.notna(sub_data_all.at[4, "chrcdss_calg4"]) and pd.notna(sub_data_all.at[4, "chrcdss_calg5"]) and pd.notna(sub_data_all.at[4, "chrcdss_calg6"]) and pd.notna(sub_data_all.at[4, "chrcdss_calg7"]) and pd.notna(sub_data_all.at[4, "chrcdss_calg8"]) and pd.notna(sub_data_all.at[4, "chrcdss_calg9"]):
					
					print("No missin data yay in month2")
					cdss_m2 = 1
					form_info_2.at["no_missing_cdss", month2] = "1" #none are nan so not missing
				else: 
					print("Missing variables")
					form_info_2.at["no_missing_cdss", month2] = "0"
			else:
				form_info_2.at["no_missing_cdss", month2] = "0"


	if name in ['oasis'] and 2 in sub_data_all.index:
		# baseline
		if "chroasis_oasis_1" not in sub_data_all or  "chroasis_oasis_2" not in sub_data_all or "chroasis_oasis_3" not in sub_data_all or "chroasis_oasis_4" not in sub_data_all or "chroasis_oasis_2" not in sub_data_all:
			print("Missing variables nay")
			form_info_2.at["no_missing_oasis", baseline] = "0" # something missing
			form_info_2.at["no_missing_oasis", month2] = "0"
		else:
			if pd.notna(sub_data_all.at[2, "chroasis_oasis_1"]) and pd.notna(sub_data_all.at[2, "chroasis_oasis_2"]) and pd.notna(sub_data_all.at[2, "chroasis_oasis_3"]) and pd.notna(sub_data_all.at[2, "chroasis_oasis_4"]) and pd.notna(sub_data_all.at[2, "chroasis_oasis_5"]):
				print("yay")
				oasis_bl = 1
				form_info_2.at["no_missing_oasis", baseline] = "1" # nothing missing or na
			else: 
				form_info_2.at["no_missing_oasis", baseline] = "0"
			#month2
			if 4 in sub_data_all.index:
				if pd.notna(sub_data_all.at[4, "chroasis_oasis_1"]) and pd.notna(sub_data_all.at[4, "chroasis_oasis_2"]) and pd.notna(sub_data_all.at[4, "chroasis_oasis_3"]) and pd.notna(sub_data_all.at[4, "chroasis_oasis_4"]) and pd.notna(sub_data_all.at[4, "chroasis_oasis_5"]):
					print("yay in month2")
					oasis_m2 = 1
					form_info_2.at["no_missing_oasis", month2] = "1" # nothing missing or na
				else: 
					form_info_2.at["no_missing_oasis", month2] = "0"
			else:
				form_info_2.at["no_missing_oasis", month2] = "0"
			
	if name in ['pss'] and 2 in sub_data_all.index: 
		# baseline
		if "chrpss_pssp1_1" not in sub_data_all or  "chrpss_pssp1_1" not in sub_data_all or "chrpss_pssp1_2" not in sub_data_all or "chrpss_pssp1_3" not in sub_data_all or "chrpss_pssp2_1" not in sub_data_all or "chrpss_pssp2_3" not in sub_data_all or "chrpss_pssp2_4" not in sub_data_all or "chrpss_pssp2_5" not in sub_data_all or "chrpss_pssp3_1" not in sub_data_all or "chrpss_pssp3_4" not in sub_data_all:
			print("Missing variables")
			form_info_2.at["no_missing_pss", baseline] = "0" #missing data
		else:
			if pd.isnull(sub_data_all.at[2, "chrpss_pssp1_1"]) or pd.isnull(sub_data_all.at[2, "chrpss_pssp1_2"]) or pd.isnull(sub_data_all.at[2, "chrpss_pssp1_3"]) or pd.isnull(sub_data_all.at[2, "chrpss_pssp2_1"]) or pd.isnull(sub_data_all.at[2, "chrpss_pssp2_2"]) or pd.isnull(sub_data_all.at[2, "chrpss_pssp2_3"]) or pd.isnull(sub_data_all.at[2, "chrpss_pssp2_4"]) or pd.isnull(sub_data_all.at[2, "chrpss_pssp2_5"]) or pd.isnull(sub_data_all.at[2, "chrpss_pssp3_1"]) or pd.isnull(sub_data_all.at[2, "chrpss_pssp3_4"]):						
				form_info_2.at["no_missing_pss", baseline] = "0"
			else: 
				form_info_2.at["no_missing_pss", baseline] = "1" #none are null

	if name in ['item_promis_for_sleep'] and 2 in sub_data_all.index: 
		# baseline
		if "chrpromis_sleep109" not in sub_data_all or "chrpromis_sleep109" not in sub_data_all or "chrpromis_sleep116" not in sub_data_all or "chrpromis_sleep20" not in sub_data_all or "chrpromis_sleep44" not in sub_data_all or "chrpromise_sleep108" not in sub_data_all or "chrpromis_sleep72" not in sub_data_all or "chrpromis_sleep67" not in sub_data_all or "chrpromis_sleep115" not in sub_data_all:
			print("Missing variables")
			form_info_2.at["no_missing_promis", baseline] = "0"
			form_info_2.at["no_missing_promis", month2] = "0"
		else:			
			if pd.isnull(sub_data_all.at[2, "chrpromis_sleep109"]) or pd.isnull(sub_data_all.at[2, "chrpromis_sleep116"]) or pd.isnull(sub_data_all.at[2, "chrpromis_sleep20"]) or pd.isnull(sub_data_all.at[2, "chrpromis_sleep44"]) or pd.isnull(sub_data_all.at[2, "chrpromise_sleep108"]) or pd.isnull(sub_data_all.at[2, "chrpromis_sleep72"]) or pd.isnull(sub_data_all.at[2, "chrpromis_sleep67"]) or pd.isnull(sub_data_all.at[2, "chrpromis_sleep115"]):
				
				print("Missing data")	
				form_info_2.at["no_missing_promis", baseline] = "0"
				
			else: 
				print("No missing data")
				promis_bl = 1
				form_info_2.at["no_missing_promis", baseline] = "1"
			#month2
			if 4 in sub_data_all.index:
				if pd.isnull(sub_data_all.at[4, "chrpromis_sleep109"]) or pd.isnull(sub_data_all.at[4, "chrpromis_sleep116"]) or pd.isnull(sub_data_all.at[4, "chrpromis_sleep20"]) or pd.isnull(sub_data_all.at[4, "chrpromis_sleep44"]) or pd.isnull(sub_data_all.at[4, "chrpromise_sleep108"]) or pd.isnull(sub_data_all.at[4, "chrpromis_sleep72"]) or pd.isnull(sub_data_all.at[4, "chrpromis_sleep67"]) or pd.isnull(sub_data_all.at[4, "chrpromis_sleep115"]):
				
					print("Missing data")	
					form_info_2.at["no_missing_promis", month2] = "0"
				
				else: 
					print("No missing data")
					promis_m2 = 1
					form_info_2.at["no_missing_promis", month2] = "1"
			else:
				form_info_2.at["no_missing_promis", month2] = "0"


	if name in ['bprs'] and 2 in sub_data_all.index: 
		# baseline
		if "chrbprs_bprs_somc" in sub_data_all:
			if "chrbprs_bprs_total" not in sub_data_all: 
				form_info_2.at["no_missing_bprs", baseline] = "0"
				form_info_2.at["no_missing_bprs", month2] = "0"
			else:
				if pd.notna(sub_data_all.at[2, "chrbprs_bprs_total"]) and sub_data_all.at[2, "chrbprs_bprs_total"] != 999:
					bprs_bl = 1					
					form_info_2.at["no_missing_bprs", baseline] = "1"
				else: 
					form_info_2.at["no_missing_bprs", baseline] = "0"
				if 4 in sub_data_all.index:	
					if pd.notna(sub_data_all.at[4, "chrbprs_bprs_total"]) and sub_data_all.at[4, "chrbprs_bprs_total"] != 999:
						bprs_m2 = 1
						form_info_2.at["no_missing_bprs", month2] = "1"
					else: 
						form_info_2.at["no_missing_bprs", month2] = "0"
				else:
					form_info_2.at["no_missing_bprs", month2] = "0"


		#calculating clinical status
		bl_clin = 0
		m2_clin = 0
		if bprs_bl == 1 and oasis_bl == 1 and cdss_bl == 1 and promis_bl == 1 and nsipr_bl == 1:

			bl_clin = 1
		if bprs_m2 == 1 and oasis_m2 == 1 and cdss_m2 == 1 and promis_m2 == 1 and nsipr_m2 == 1:
			m2_clin = 1

		print("Baseline Clinical: " + str(bl_clin))
		print([bprs_bl, oasis_bl, cdss_bl, promis_bl, nsipr_bl, cssrs_bl])
		print("M2 Clinical: " + str(m2_clin))
		print([bprs_m2, oasis_m2, cdss_m2, promis_m2, nsipr_m2, cssrs_m2])

		if bl_clin == 1 and m2_clin == 1:
			print("Both baseline and month 2 clin")
			clinical_status = 3
			form_info_2.at["clinical_status", screening] = 3
		if bl_clin == 1 and m2_clin == 0:
			print("Only baseline clin")
			clinical_status = 1
			form_info_2.at["clinical_status", screening] = 1
		if bl_clin == 0 and m2_clin == 1:
			print("ONly month 2 cog")
			clinical_status = 2
			form_info_2.at["clinical_status", screening] = 2
		if bl_clin == 0 and m2_clin == 0:
			print("Not completed")
			clinical_status = 0
			form_info_2.at["clinical_status", screening] = 0


	if name in ['cdssrs_baseline'] and 2 in sub_data_all.index: 
		# baseline
		if "chrcssrsb_si1l" not in sub_data_all or "chrcssrsb_si2l" not in sub_data_all or "chrcssrsb_css_sim1" not in sub_data_all or "chrcssrsb_css_sim2" not in sub_data_all:
				print("Missing variables")
				form_info_2.at["no_missing_cdssrsb", baseline] = "0"
		else:
			if pd.notna(sub_data_all.at[2, "chrcssrsb_si1l"]) and sub_data_all.at[2, "chrcssrsb_si2l"] == "2": 
				cssrs_bl = 1
				if (sub_data_all.at[2, "chrcssrsb_css_sim1"] == "1" or sub_data_all.at[2, "chrcssrsb_css_sim2"] == "1") and (pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim3"]) and pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim4"]) and pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim5"])):
					print("No missing")
					form_info_2.at["no_missing_cdssrsb", baseline] = "1"
				elif sub_data_all.at[2, "chrcssrsb_css_sim1"] == "0" and sub_data_all.at[2, "chrcssrsb_css_sim2"] == "0":
					print("yay")
					cssrs_bl = 1
					form_info_2.at["no_missing_cdssrsb", baseline] = "1"
				else:
					form_info_2.at["no_missing_cdssrsb", baseline] = "0"

			elif pd.notna(sub_data_all.at[2, "chrcssrsb_si1l"]) and sub_data_all.at[2, "chrcssrsb_si2l"] == "1" and sub_data_all.at[2, "chrcssrsb_si3l"] == "2":
				cssrs_bl = 1
				if (sub_data_all.at[2, "chrcssrsb_css_sim1"] == "1" or sub_data_all.at[2, "chrcssrsb_css_sim2"] == "1") and (pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim3"]) and pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim4"]) and pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim5"])):
					form_info_2.at["no_missing_cdssrsb", baseline] = "1"
				elif sub_data_all.at[2, "chrcssrsb_css_sim1"] == "0" and sub_data_all.at[2, "chrcssrsb_css_sim2"] == "0":
					cssrs_bl = 1
					form_info_2.at["no_missing_cdssrsb", baseline] = "1"
				else:
					form_info_2.at["no_missing_cdssrsb", baseline] = "0"
			elif pd.notna(sub_data_all.at[2, "chrcssrsb_si1l"]) and sub_data_all.at[2, "chrcssrsb_si2l"] == "1" and sub_data_all.at[2, "chrcssrsb_si3l"] == "1" and sub_data_all.at[2, "chrcssrsb_si4l"] == "2":
				cssrs_bl = 1
				if (sub_data_all.at[2, "chrcssrsb_css_sim1"] == "1" or sub_data_all.at[2, "chrcssrsb_css_sim2"] == "1") and (pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim3"]) and pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim4"]) and pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim5"])):
					form_info_2.at["no_missing_cdssrsb", baseline] = "1"
				elif sub_data_all.at[2, "chrcssrsb_css_sim1"] == "0" and sub_data_all.at[2, "chrcssrsb_css_sim2"] == "0":
					print("yay")
					cssrs_bl = 1
					form_info_2.at["no_missing_cdssrsb", baseline] = "1"
				else:
					form_info_2.at["no_missing_cdssrsb", baseline] = "0"

			elif pd.notna(sub_data_all.at[2, "chrcssrsb_si1l"]) and sub_data_all.at[2, "chrcssrsb_si2l"] == "1" and sub_data_all.at[2, "chrcssrsb_si3l"] == "1" and sub_data_all.at[2, "chrcssrsb_si4l"] == "1" and pd.notna(sub_data_all.at[2, "chrcssrsb_si5l"]):
				cssrs_bl = 1
				if (sub_data_all.at[2, "chrcssrsb_css_sim1"] == "1" or sub_data_all.at[2, "chrcssrsb_css_sim2"] == "1") and (pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim3"]) and pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim4"]) and pd.notna(sub_data_all.at[2, "chrcssrsb_css_sim5"])):
					form_info_2.at["no_missing_cdssrsb", baseline] = "1"
				elif sub_data_all.at[2, "chrcssrsb_css_sim1"] == "0" and sub_data_all.at[2, "chrcssrsb_css_sim2"] == "0":
					cssrs_bl = 1
					form_info_2.at["no_missing_cdssrsb", baseline] = "1"
				else:
					form_info_2.at["no_missing_cdssrsb", baseline] = "0"

			else: 
				print("Missing")
				form_info_2.at["no_missing_cdssrsb", baseline] = "0"

	if name in ['cdssrs_followup'] and 4 in sub_data_all.index: 
		# baseline
		if "chrcssrsfu_css_sim1" not in sub_data_all or "chrcssrsfu_css_sim2" not in sub_data_all or "chrcssrsfu_css_sim3" not in sub_data_all or "chrcssrsfu_css_sim4" not in sub_data_all or "chrcssrsfu_css_sim5" not in sub_data_all:	
			print("Missing variables")
			form_info_2.at["no_missing_cdssrsfu", month2] = "0"
		else:
			if pd.notna(sub_data_all.at[4, "chrcssrsfu_css_sim1"]) and sub_data_all.at[4, "chrcssrsfu_css_sim2"] == "0": 
				print("correct")
				cssrs_m2 = 1
				form_info_2.at["no_missing_cdssrsfu", month2] = "1"

			elif pd.notna(sub_data_all.at[4, "chrcssrsfu_css_sim1"]) and sub_data_all.at[4, "chrcssrsfu_css_sim2"] == "1" and sub_data_all.at[4, "chrcssrsfu_css_sim3"] == "0":
				print("correct")
				cssrs_m2 = 1
				form_info_2.at["no_missing_cdssrsfu", month2] = "1"
					
			elif pd.notna(sub_data_all.at[4, "chrcssrsfu_css_sim1"]) and sub_data_all.at[4, "chrcssrsfu_css_sim2"] == "1" and sub_data_all.at[4, "chrcssrsfu_css_sim3"] == "1" and sub_data_all.at[4, "chrcssrsfu_css_sim4"] == "0":
				print("correct")
				form_info_2.at["no_missing_cdssrsfu", month2] = "1"

			elif pd.notna(sub_data_all.at[4, "chrcssrsfu_css_sim1"]) and sub_data_all.at[4, "chrcssrsfu_css_sim2"] == "1" and sub_data_all.at[4, "chrcssrsfu_css_sim3"] == "1" and sub_data_all.at[4, "chrcssrsfu_css_sim4"] == "1" and pd.notna(sub_data_all.at[4, "chrcssrsfu_css_sim5"]):
				print("correct")
				cssrs_m2 = 1
				form_info_2.at["no_missing_cdssrsfu", month2] = "1"
				

			else: 
				print("incorrect")
				form_info_2.at["no_missing_cdssrsfu", month2] = "0"



	if name in ['premorbid_adjustment_scale'] and 2 in sub_data_all.index: 
		# baseline
		pas_6_11_q = 0
		pas_12_15_q = 0
		pas_16_18_q = 0
		pub_19_q = 0
		if "chrpas_pmod_child1" not in sub_data_all or  "chrpas_pmod_child1" not in sub_data_all or "chrpas_pmod_child2" not in sub_data_all or "chrpas_pmod_child3" not in sub_data_all or "chrpas_pmod_child4" not in sub_data_all:
			print("Missing variables")
			pas_6_11_q = 0
		else:
			if pd.isnull(sub_data_all.at[3, "chrpas_pmod_child1"]) or pd.isnull(sub_data_all.at[3, "chrpas_pmod_child2"]) or pd.isnull(sub_data_all.at[3, "chrpas_pmod_child3"]) or pd.isnull(sub_data_all.at[3, "chrpas_pmod_child4"]):
				pas_6_11_q = 0
			else:
				print("Complete variables")
				pas_6_11_q = 1
			# early adol
		if "chrpas_pmod_adol_early1" not in sub_data_all or "chrpas_pmod_adol_early2" not in sub_data_all or "chrpas_pmod_adol_early3" not in sub_data_all or "chrpas_pmod_adol_early4" not in sub_data_all or "chrpas_pmod_adol_early5" not in sub_data_all:
			pub_12_15_q = 0
		else:
			if pd.notna(sub_data_all.at[3, "chrpas_pmod_adol_early1"]) or pd.notna(sub_data_all.at[3, "chrpas_pmod_adol_early2"]) or pd.notna(sub_data_all.at[3, "chrpas_pmod_adol_early3"]) or pd.notna(sub_data_all.at[3, "chrpas_pmod_adol_early4"]) or pd.notna(sub_data_all.at[3, "chrpas_pmod_adol_early5"]) or pd.isnull(sub_data_all.at[3, "chrpas_pmod_child4"]):					
				print("Not missing early adol data")
				pub_12_15_q = 1

			#late adol
		if "chrpas_pmod_adol_late1" not in sub_data_all or "chrpas_pmod_adol_late2" not in sub_data_all or "chrpas_pmod_adol_late3" not in sub_data_all or "chrpas_pmod_adol_late4" not in sub_data_all or "chrpas_pmod_adol_late5" not in sub_data_all:
				pub_16_18_q = 0
		else:
			if pd.notna(sub_data_all.at[3, "chrpas_pmod_adol_late1"]) or pd.notna(sub_data_all.at[3, "chrpas_pmod_adol_late2"]) or pd.notna(sub_data_all.at[3, "chrpas_pmod_adol_late3"]) or pd.notna(sub_data_all.at[3, "chrpas_pmod_adol_late4"]) or pd.notna(sub_data_all.at[3, "chrpas_pmod_adol_late5"]):
				print("Not missing late adol")	
				pub_16_18_q = 1			

#adult
		if "chrpas_pmod_adult1" not in sub_data_all or "chrpas_pmod_adult2" not in sub_data_all or ("chrpas_pmod_adult3v1" not in sub_data_all and "chrpas_pmod_adult3v3" not in sub_data_all):
			pub_19_q = 0
		else:
			if pd.isnull(sub_data_all.at[3, "chrpas_pmod_adult1"]) or pd.isnull(sub_data_all.at[3, "chrpas_pmod_adult2"]):
				print("Not missing adult data")
				pub_19_q = 1					
			else:
				pub_19_q = 0	
		# all questions
		form_info_2.at["no_missing_pubds", baseline] = "0"
		if age < 15 and pas_6_11_q == 1:	
			form_info_2.at["no_missing_pubds", month1] = "1"
		if age < 18 and pas_6_11_q == 1 and pub_12_15_q == 1:
			form_info_2.at["no_missing_pubds", month1] = "1"
		if age < 19 and pas_6_11_q == 1 and pub_12_15_q == 1 and pub_16_18_q == 1:
			form_info_2.at["no_missing_pubds", month1] = "1"
		if age > 19 and pas_6_11_q == 1 and pub_12_15_q == 1 and pub_16_18_q == 1 and pub_19_q == 1:
			form_info_2.at["no_missing_pubds", month1] = "1"
			
			

	form_info_2 = form_info_2.transpose()
	form_info_2.reset_index(inplace=True)
	#print("Printing form info - are there multiple rows?")
	#print(form_info_2)

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
		#print(int_avail)
		#print(ent_avail)

		# if interview is available and in the data
		if len(int_avail) > 0 and int_avail[0] in sub_data_all and name not in ['mri_run_sheet', 'current_health_status', 'psychs_p1p8_fu'] :
			var = int_avail[0]
			sub_data_test = sub_data_all[sub_data_all['redcap_event_name'].isin([event_2])]
			sub_data_test = sub_data_test.reset_index(drop=True)
			int_date = sub_data_test.at[0, var]
			#print('Use interview date' + str(int_date))

			if pd.notna(int_date) and int_date != '-9' and int_date != '-3':
				dpdash_main.at[event_2, 'mtime'] = int_date
				#print(str(int_date))
				day = days_between(consent, int_date) + 1
			else:
				day = 0	## remove days with 0 at end		
			
		else:
			#print("No interview date")
			if event_2 in ['screening_arm_1']: # works for floating forms, lifetime AP
				dpdash_main.at[event_2, 'mtime'] = consent
				day = 1
			else:
				day = 2

		
		if name in ['psychs_p1p8_fu'] or name in ['psychs_p9ac32_fu']:
			if 'chrpsychs_fu_interview_date' in sub_data_all:
				#print("CHECK")
				sub_data_test = sub_data_all[sub_data_all['redcap_event_name'].isin([event_2])]
				sub_data_test = sub_data_test.reset_index(drop=True)
				int_date = sub_data_test.loc[0, "chrpsychs_fu_interview_date"]
				#print('Use interview date' + str(int_date))
				if pd.notna(int_date) and int_date != '-9' and int_date != '-3':
					dpdash_main.at[event_2, 'mtime'] = int_date
					print(str(int_date))
					day = days_between(consent, int_date) + 1
				else:
					day = 0	## remove days with 0 at end	
			else:
				day = 0	## remove days with 0 at end
			
		# use entry date for a couple forms that interview date doesn't work
		# check coenrollment_form
		if name in ['mri_run_sheet', 'current_health_status', 'premorbid_iq_reading_accuracy'] and ent_avail[0] in sub_data_all:
			#print("need entry date")
			var = ent_avail[0]
			sub_data_test = sub_data_all[sub_data_all['redcap_event_name'].isin([event_2])]
			sub_data_test = sub_data_test.reset_index(drop=True)
			ent_date = sub_data_test.at[0, var]
			
			if pd.notna(ent_date) and ent_date != "-3" and ent_date != "-9":
				dpdash_main.at[event_2, 'mtime'] = ent_date
				day = days_between(consent, ent_date) + 1

			else:
				day = 0
				

		# setting day as the difference between the consent date (day 1) and interview date
		#print("THE DAY FOR THE FORM: " + str(day))
		dpdash_main.at[event_2, 'day'] = day
	
		# adding a days since consent
		dpdash_main.at[event_2, 'days_since_consent'] = days_between(consent, today)	


	#removing rows with no data
	final_csv = dpdash_main[dpdash_main.Percentage != 0]
	#print("After removing because of percentage but before removing duplicates")
	#print(final_csv)
	final_csv = final_csv.drop_duplicates()
	final_csv = final_csv[final_csv.day != 0]
	#print("After removing duplicates and day 0")
	#print(final_csv)

	#final_csv = dpdash_main
	last_day = final_csv['day'].max()
	if pd.isna(last_day):
		last_day = 1
	
	if not pd.isna(last_day) and int(last_day) > 300:
		print("ALERT: ERROR WIITH DATE IN FORM - Last day too high")
		last_day = 300
	#print("LAST DAY for file: " + str(last_day))

	if 'lifetime_ap_exposure_screen' in final_csv:
		print("Rounding ap exposure")
		final_csv[["chrap_total"]] = final_csv[["chrap_total"]].apply(pd.to_numeric)
		print(final_csv[["chrap_total"]])
		final_csv = final_csv.round({"chrap_total":1})
		print(final_csv[["chrap_total"]])

	#print("Printing final csv and day for: ", name)	
	#print(final_csv.T)

	#print(final_csv['day'])
	
	# Saving to a csv based on ID and event
	final_csv.to_csv(output1 + site+"-"+id+"-form_"+name+'-day1to'+str(last_day)+".csv", sep=',', index = False, header=True)



print(percentage_form_2.T)
percentage_form_2.index.name='Index'
percentage_form_2.to_csv(output1 + site+"-"+id+"-percentage.csv", sep=',', index = True, header=True)


