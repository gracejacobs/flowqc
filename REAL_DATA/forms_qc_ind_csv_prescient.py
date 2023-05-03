import sys
import numpy as np
import pandas as pd
import argparse
import json
import os
from datetime import datetime
from datetime import date

def days_between(d1, d2):
    return abs((d2 - d1).days)

today = date.today()
today = today.strftime("%Y-%m-%d")

id = str(sys.argv[1])
print("ID: ", id)

site = id[0:2]
print("Site: ", site)

#output1 = "/data/predict/data_from_nda/formqc/"
output1 = "/data/predict1/data_from_nda/formqc/"
#output_processed = "/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient{0}/processed/{1}/surveys/".format(site, id)

# creating a folder if there isn't one#
#if not os.path.exists(output_processed):
#	print("Creating output directory in participant's processed directory")
#	os.makedirs(output_processed)

input_path = "/data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient{0}/raw/{1}/surveys/".format(site, id)
print("Participant folder: ", input_path)

print("Loading data dictionary")
dict = pd.read_csv('/data/pnl/home/gj936/U24/Clinical_qc/flowqc/AMPSCZFormRepository_DataDictionary_2023-01-19_min.csv', sep= ",", index_col = False, low_memory=False)

status = "0"
percpath = (output1 + site+"-"+id+"-percentage.csv")
print("Loading percent file")
if os.path.exists(percpath):
	perc_check = pd.read_csv(percpath)
	perc_check = perc_check.fillna(0)
	#print(perc_check)
	perc_check = perc_check.drop('informed_consent_run_sheet' , axis='columns')
	perc_check = perc_check[perc_check["Unnamed: 0"] != 99]
	perc_check['Completed']= perc_check.iloc[:, 1:].sum(axis=1)
	perc_check['Total_empty'] = (perc_check == 0).sum(axis=1)
	with pd.option_context('display.max_rows', None, 'display.precision', 3,):
		print(perc_check)
	perc_check_2 = perc_check[perc_check.Completed > 200]
	#checking for conversion
	perc_check = perc_check[perc_check["Unnamed: 0"] == 98]
	perc_check = perc_check.reset_index()
	print(perc_check.transpose())

	if perc_check_2.empty:
		status = "0"
	else:
		status = perc_check_2["Unnamed: 0"].iloc[-1]

		if perc_check_2['Total_empty'].min() > 58:
			status = "0"

	if not perc_check.empty:		
		if perc_check['Completed'].min() > 100:
			status = "98" #conversion


	print("VISIT STATUS: " + str(status))


# Getting all the form names from the data dictionary
#form_names = pd.read_csv('form_names.csv')
form_names = dict['Form Name'].unique()
print("check")
percentage_form = pd.DataFrame(columns = form_names)
form_names = np.append(form_names, "chart_statuses")

print("Loading consent sheet")
file = input_path+id+"_informed_consent_run_sheet.csv"
sub_consent = pd.read_csv(file)
print(sub_consent)
consent = sub_consent.at[0, "chric_consent_date"]
consent = consent.split(" ")[0]

print("Participant consent date: " + consent)
consent = datetime.strptime(consent, "%d/%m/%Y")

# Subsetting data based on event
baseline_tracker = {}
screening_tracker = {}
month1_tracker = {}
month2_tracker = {}
month3_tracker = {}
month4_tracker = {}
month5_tracker = {}
month6_tracker = {}
month7_tracker = {}
month8_tracker = {}
conversion_tracker = {}

bl_wasi = 0
bl_preiq = 0
bl_penn = 0
m2_penn = 0

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

age_chart = np.nan
sex_chart = np.nan
included = np.nan
cognition_status = 0
blood_status = 0
saliva_status = 0
clinical_status = 0
date_drawn_bl = np.nan
date_ate_bl = np.nan
date_drawn_m2 = np.nan
date_ate_m2 = np.nan

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

	if name in ["blood_sample_preanalytic_quality_assurance"]: # using combined version of 3 csvs
		file = input_path+id+"_blood_sample_preanalytic_quality_assurance.csv.flat"	
		print("Alternative name for the blood: " + file)
	else:
		file = input_path+id+"_"+name+".csv"
		
	if os.path.isfile(file): 
		sub_data_all = pd.read_csv(file)
		#print(sub_data_all)
		#sub_data_all = sub_data_all.replace('-3', np.NaN, regex=True)
		#sub_data_all = sub_data_all.replace(-3, np.NaN, regex=True)
		#print(sub_data_all['visit'].unique())
		check = 1

		for event in sub_data_all['visit'].unique():
			event = str(event)
			#print("Event: " + event)
			sub_data = sub_data_all[sub_data_all['visit'].astype(str).isin([event])]
			sub_data = sub_data.reset_index(drop=True)
			#sub_data = sub_data.apply(lambda x: x.str.strip()).replace('', np.nan)

			form_info = pd.DataFrame(columns = ['Variables'])
			form = dict.loc[dict['Form Name'] == name]
			form_vars = form['Variable / Field Name'].tolist()

			# Adding all variables to the csv
			# PROBLEM - the data dictionary is not up to date - chance that if I update the data dictionary, it will cause problems
			# or could just take the entire form
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

			form_info.at["Visit", 'Variables'] = event

			#print(form_info)

			## Adding percentage data
			percentage_form.at[event, name] = form_info.at["Percentage", 'Variables']


			# adding visit status
			if name in ['informed_consent_run_sheet']: 
				form_info.at["visit_status", 'Variables'] = status
				consent_age = sub_data.at[0, "interview_age"]

				form_info.at["consent_age", 'Variables'] = round((int(consent_age)/12), 1)
				#print(form_info) 

			# adding included/excluded variables
			# creating a single exclusion inclusion variable
			if name in ['inclusionexclusion_criteria_review']: 
				form_info.at["included_excluded", 'Variables'] = np.nan
				form_info.at["Percentage", 'Variables'] = 100

				if "chrcrit_included" in sub_data and sub_data.loc[0, "chrcrit_included"] == 1:
					print("INCLUDED")
					included = 1
					form_info.at["included_excluded", 'Variables'] = 1
		
				if"chrcrit_included" in sub_data and sub_data.loc[0, "chrcrit_included"] == 0:
					print("EXCLUDED")
					included = 0					
					form_info.at["included_excluded", 'Variables'] = 0

				#print(form_info)

			# adding interview age
			age = 0
			if name in ['sociodemographics'] and pd.notna(sub_data.loc[0, "interview_age"]): 
				#print(sub_data.T)
				age = sub_data.loc[0, "interview_age"]
				print("Age: " + str(age))
				age = round((int(age)/12), 1)
				form_info.at["interview_age", 'Variables'] = age
				age_chart = age
				sex_chart = sub_data.loc[0, "chrdemo_sexassigned"] 	
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

			# TBI
			if name in ['traumatic_brain_injury_screen']:
				#print(sub_data.T)
				form_info.at["chrtbi_number_injs", 'Variables'] = sub_data.at[0, "chrtbi_subject_times"]
				form_info.at["chrtbi_severe_inj", 'Variables'] = sub_data.at[0, "chrtbi_subject_symptoms"]
				#print(form_info)


			## setting up cognition status
			if name in ['iq_assessment_wasiii_wiscv_waisiv']: 
				if "chriq_fsiq" in sub_data:
					if event == '2' and pd.notna(sub_data.at[0, "chriq_fsiq"]) and sub_data.at[0, "chriq_fsiq"] != -3 and sub_data.at[0, "chriq_fsiq"] != -9:
						print("Baseline cog complete")
						bl_wasi = 1

			if name in ['premorbid_iq_reading_accuracy']: 
				if "chrpreiq_total_raw" in sub_data:
					if event == '2' and pd.notna(sub_data.at[0, "chrpreiq_total_raw"]) and sub_data.at[0, "chrpreiq_total_raw"] != -3 and sub_data.at[0, "chrpreiq_total_raw"] != -9:
						print("Baseline cog complete")
						bl_preiq = 1

			if name in ['penncnb']: 
				if "chrpenn_complete" in sub_data:
					if event == '2' and pd.notna(sub_data.at[0, "chrpenn_complete"]) and sub_data.at[0, "chrpenn_complete"] != -3 and sub_data.at[0, "chrpenn_complete"] != -9 and sub_data.at[0, "chrpenn_complete"] != 3:
						print("Baseline cog complete")
						bl_penn = 1
					if event == '4' and pd.notna(sub_data.at[0, "chrpenn_complete"]) and sub_data.at[0, "chrpenn_complete"] != -3 and sub_data.at[0, "chrpenn_complete"] != -9 and sub_data.at[0, "chrpenn_complete"] != 3:
						print("Month 1 cog complete")
						m2_penn = 1
				
			
				

			
		############ Missing variables for clinical data
			if name in ['perceived_discrimination_scale']: 
		# baseline 
				if "chrdim_dim_yesno_q1_1" not in sub_data or "chrdlm_dim_yesno_q1_2" not in sub_data or "chrdlm_dim_sex" not in sub_data or "chrdlm_dim_yesno_age" not in sub_data or "chrdlm_dim_yesno_q4_1" not in sub_data or "chrdlm_dim_yesno_q5" not in sub_data or "chrdlm_dim_yesno_q3" not in sub_data or "chrdlm_dim_yesno_q6" not in sub_data or "chrdlm_dim_yesno_other" not in sub_data:
					#print("Missing variables in json: " + value)
					form_info.at["no_missing_pdiscrims", 'Variables'] = 0 # missing data
	
				else: # still could be missing data - check if any are null
					print("Not missing variables in json")
					form_info.at["no_missing_pdiscrims", 'Variables'] = 0

					if pd.isnull(sub_data_all.at[0, "chrdim_dim_yesno_q1_1"]) or pd.isnull(sub_data_all.at[0, "chrdlm_dim_yesno_q1_2"]) or pd.isnull(sub_data_all.at[0, "chrdlm_dim_sex"]) or pd.isnull(sub_data_all.at[0, "chrdlm_dim_yesno_age"]) or pd.isnull(sub_data_all.at[0, "chrdlm_dim_yesno_q4_1"]) or pd.isnull(sub_data_all.at[0, "chrdlm_dim_yesno_q5"]) or pd.isnull(sub_data_all.at[0, "chrdlm_dim_yesno_q3"]) or pd.isnull(sub_data_all.at[0, "chrdlm_dim_yesno_q6"]) or pd.isnull(sub_data_all.at[0, "chrdlm_dim_yesno_other"]):			
						print("One of the variables is missing")
						form_info.at["no_missing_pdiscrims", 'Variables'] = 0
					else: 
						print("None of the variables are missing")
						form_info.at["no_missing_pdiscrims", 'Variables'] = "1"

			if name in ['pubertal_developmental_scale']:
				# baseline
				pub_first_q = 0
				pub_m_q = 0
				pub_f_q = 0
				print("First check if variables exist")
				if "chrpds_pds_1_p" in sub_data and "chrpds_pds_1_p" in sub_data and "chrpds_pds_2_p" in sub_data and "chrpds_pds_3_p" in sub_data:
					print("No missing variables")
					pub_first_q = 0
					if pd.isnull(sub_data.at[0, "chrpds_pds_1_p"]) or pd.isnull(sub_data.at[0, "chrpds_pds_2_p"]) or  pd.isnull(sub_data.at[0, "chrpds_pds_3_p"]):
						print("One is null")
						pub_first_q = 0 # missing data
					else:
						print("No missing variables")
						pub_first_q = 1 # has all data

				# male questions
				if "chrdemo_sexassigned" in sub_data and sub_data.at[0, "chrdemo_sexassigned"] == "1" and "chrpds_pds_1_p" in sub_data and "chrpds_pds_m5_p" in sub_data:
					if pd.isnull(sub_data.at[0, "chrpds_pds_m4_p"]) or pd.isnull(sub_data.at[0, "chrpds_pds_m5_p"]):
						print("Missing male variables")
						pub_m_q = 0 # missiing data
					else:
						print("No missing male variables")
						pub_m_q = 1 # looks like has data
				# female questions
				if "chrdemo_sexassigned" in sub_data and sub_data.at[0, "chrdemo_sexassigned"] == "2" and "chrpds_pds_f4_p" in sub_data and "chrpds_pds_f5b_p" in sub_data:
					if pd.isnull(sub_data.at[0, "chrpds_pds_f4_p"]) or pd.isnull(sub_data.at[0, "chrpds_pds_f5b_p"]):
						print("Missing female variables")
						pub_f_q = 0 #missing data
					else:
						pub_f_q = 1 #not missing data
				# all questions
				if pub_first_q == 1 and pub_m_q == 1 or pub_f_q == 1:
					print("Not missing variables")
					form_info.at["no_missing_pubds", 'Variables'] = "1"
				else:
					form_info.at["no_missing_pubds", 'Variables'] = "0"

			if name in ['nsipr']:
			# baseline
				nsipr_stat = 0
				if "chrnsipr_item1_rating" not in sub_data or "chrnsipr_item2_rating" not in sub_data or "chrnsipr_item3_rating" not in sub_data or "chrnsipr_item4_rating" not in sub_data or "chrnsipr_item5_rating" not in sub_data or "chrnsipr_item6_rating" not in sub_data or "chrnsipr_item7_rating" not in sub_data or "chrnsipr_item8_rating" not in sub_data or "chrnsipr_item9_rating" not in sub_data or "chrnsipr_item10_rating" not in sub_data or "chrnsipr_item11_rating" not in sub_data:
					print("Missing variables")
					form_info.at["no_missing_nsipr", 'Variables'] = "0" # missing data
				else:
					if pd.notna(sub_data.at[0, "chrnsipr_item1_rating"]) and pd.notna(sub_data.at[0, "chrnsipr_item2_rating"]) and pd.notna(sub_data.at[0, "chrnsipr_item3_rating"]) and pd.notna(sub_data.at[0, "chrnsipr_item4_rating"]) and pd.notna(sub_data.at[0, "chrnsipr_item5_rating"]) and pd.notna(sub_data.at[0, "chrnsipr_item6_rating"]) and pd.notna(sub_data.at[0, "chrnsipr_item7_rating"]) and pd.notna(sub_data.at[0, "chrnsipr_item8_rating"]) and pd.notna(sub_data.at[0, "chrnsipr_item9_rating"]) and pd.notna(sub_data.at[0, "chrnsipr_item10_rating"]) and pd.notna(sub_data.at[0, "chrnsipr_item11_rating"]):
						print("yay no missing")
						nsipr_stat = 1
						form_info.at["no_missing_nsipr", 'Variables'] = "1" # none are NA
					else:
						form_info.at["no_missing_nsipr", 'Variables'] = "0"

				if event == "2":
					nsipr_bl = nsipr_stat
				if event == "4":
					nsipr_m2 = nsipr_stat


			if name in ['cdss']:
			# baseline
				cdss_stat = 0
				if "chrcdss_calg1" not in sub_data or  "chrcdss_calg2" not in sub_data or "chrcdss_calg3" not in sub_data or "chrcdss_calg4" not in sub_data or "chrcdss_calg5" not in sub_data or "chrcdss_calg6" not in sub_data or "chrcdss_calg7" not in sub_data or "chrcdss_calg8" not in sub_data or "chrcdss_calg9" not in sub_data:
					print("Missing variables")
					form_info.at["no_missing_cdss", 'Variables'] = "0" #missing data
				else:
					if pd.notna(sub_data.at[0, "chrcdss_calg1"]) and pd.notna(sub_data.at[0, "chrcdss_calg2"]) and pd.notna(sub_data.at[0, "chrcdss_calg3"]) and pd.notna(sub_data.at[0, "chrcdss_calg4"]) and pd.notna(sub_data.at[0, "chrcdss_calg5"]) and pd.notna(sub_data.at[0, "chrcdss_calg6"]) and pd.notna(sub_data.at[0, "chrcdss_calg7"]) and pd.notna(sub_data.at[0, "chrcdss_calg8"]) and pd.notna(sub_data.at[0, "chrcdss_calg9"]):

						print("No missin data yay")
						cdss_stat = 1
						form_info.at["no_missing_cdss", 'Variables'] = "1" #none are nan so not missing
					else:
						print("Missing variables")
						form_info.at["no_missing_cdss", 'Variables'] = "0"

				if event == "2":
					cdss_bl = cdss_stat
				if event == "4":
					cdss_m2 = cdss_stat

			if name in ['oasis']:
			# baseline
				oasis_stat = 0
				if "chroasis_oasis_1" not in sub_data or  "chroasis_oasis_2" not in sub_data or "chroasis_oasis_3" not in sub_data or "chroasis_oasis_4" not in sub_data or "chroasis_oasis_2" not in sub_data:
					form_info.at["no_missing_oasis", 'Variables'] = "0" # something missing
				else:
					if pd.notna(sub_data.at[0, "chroasis_oasis_1"]) and pd.notna(sub_data.at[0, "chroasis_oasis_2"]) and pd.notna(sub_data.at[0, "chroasis_oasis_3"]) and pd.notna(sub_data.at[0, "chroasis_oasis_4"]) and pd.notna(sub_data.at[0, "chroasis_oasis_5"]):
						print("yay")
						oasis_stat = 1
						form_info.at["no_missing_oasis", 'Variables'] = "1" # nothing missing or na
					else:
						form_info.at["no_missing_oasis", 'Variables'] = "0"

				if event == "2":
					oasis_bl = oasis_stat
				if event == "4":
					oasis_m2 = oasis_stat


			if name in ['perceived_stress_scale']:
			# baseline
				if "chrpss_pssp1_1" not in sub_data or  "chrpss_pssp1_1" not in sub_data or "chrpss_pssp1_2" not in sub_data or "chrpss_pssp1_3" not in sub_data or "chrpss_pssp2_1" not in sub_data or "chrpss_pssp2_3" not in sub_data or "chrpss_pssp2_4" not in sub_data or "chrpss_pssp2_5" not in sub_data or "chrpss_pssp3_1" not in sub_data or "chrpss_pssp3_4" not in sub_data:
					print("Missing variables")
					form_info.at["no_missing_pss", 'Variables'] = "0" #missing data
				else:
					if pd.isnull(sub_data.at[0, "chrpss_pssp1_1"]) or pd.isnull(sub_data.at[0, "chrpss_pssp1_2"]) or pd.isnull(sub_data.at[0, "chrpss_pssp1_3"]) or pd.isnull(sub_data.at[0, "chrpss_pssp2_1"]) or pd.isnull(sub_data.at[0, "chrpss_pssp2_2"]) or pd.isnull(sub_data.at[0, "chrpss_pssp2_3"]) or pd.isnull(sub_data.at[0, "chrpss_pssp2_4"]) or pd.isnull(sub_data.at[0, "chrpss_pssp2_5"]) or pd.isnull(sub_data.at[0, "chrpss_pssp3_1"]) or pd.isnull(sub_data.at[0, "chrpss_pssp3_4"]):
						form_info.at["no_missing_pss", 'Variables'] = "0"
					else:
						form_info.at["no_missing_pss", 'Variables'] = "1" #none are null

			if name in ['item_promis_for_sleep']:
				promis_stat = 0
				if "chrpromis_sleep109" not in sub_data or "chrpromis_sleep109" not in sub_data or "chrpromis_sleep116" not in sub_data or "chrpromis_sleep20" not in sub_data or "chrpromis_sleep44" not in sub_data or "chrpromise_sleep108" not in sub_data or "chrpromis_sleep72" not in sub_data or "chrpromis_sleep67" not in sub_data or "chrpromis_sleep115" not in sub_data:
					print("Missing variables")
					form_info.at["no_missing_promis", 'Variables'] = "0"
				else:
					if pd.isnull(sub_data.at[0, "chrpromis_sleep109"]) or pd.isnull(sub_data.at[0, "chrpromis_sleep116"]) or pd.isnull(sub_data.at[0, "chrpromis_sleep20"]) or pd.isnull(sub_data.at[0, "chrpromis_sleep44"]) or pd.isnull(sub_data.at[0, "chrpromise_sleep108"]) or pd.isnull(sub_data.at[0, "chrpromis_sleep72"]) or pd.isnull(sub_data.at[0, "chrpromis_sleep67"]) or pd.isnull(sub_data.at[0, "chrpromis_sleep115"]):
						print("Missing data")
						form_info.at["no_missing_promis", 'Variables'] = "0"

					else:
						print("No missing data")
						promis_stat = 1
						form_info.at["no_missing_promis", 'Variables'] = "1"
				
				if event == "2":
					print(str(promis_stat))
					promis_bl = promis_stat
				if event == "4":
					print(str(promis_stat))
					promis_m2 = promis_stat

			if name in ['bprs']:
				bprs_stat = 0
				if "chrbprs_bprs_somc" in sub_data:
					if "chrbprs_bprs_total" not in sub_data:
						form_info.at["no_missing_bprs", 'Variables'] = "0"
					else:
						if pd.notna(sub_data.at[0, "chrbprs_bprs_total"]) and sub_data.at[0, "chrbprs_bprs_total"] != 999:
							bprs_stat = 1
							form_info.at["no_missing_bprs", 'Variables'] = "1"
						else:
							form_info.at["no_missing_bprs", 'Variables'] = "0"

				if event == "2":
					bprs_bl = bprs_stat
				if event == "4":
					bprs_m2 = bprs_stat


			if name in ['cssrs_baseline']:
			# baseline
				if "chrcssrsb_si1l" not in sub_data or "chrcssrsb_si2l" not in sub_data or "chrcssrsb_si3l" not in sub_data or "chrcssrsb_si4l" not in sub_data or "chrcssrsb_si5l" not in sub_data or "chrcssrsb_css_sim1" not in sub_data or "chrcssrsb_css_sim2" not in sub_data or "chrcssrsb_css_sim3" not in sub_data or "chrcssrsb_css_sim4" not in sub_data or "chrcssrsb_css_sim5" not in sub_data:
					form_info.at["no_missing_cdssrsb", 'Variables'] = "0"
				else:
					if pd.notna(sub_data.at[0, "chrcssrsb_si1l"]) and pd.notna(sub_data.at[0, "chrcssrsb_si2l"]) and pd.notna(sub_data.at[0, "chrcssrsb_si3l"]) and pd.notna(sub_data.at[0, "chrcssrsb_si4l"]) and pd.notna(sub_data.at[0, "chrcssrsb_si5l"]) and pd.notna(sub_data.at[0, "chrcssrsb_css_sim1"]) and pd.notna(sub_data.at[0, "chrcssrsb_css_sim2"]) and pd.notna(sub_data.at[0, "chrcssrsb_css_sim3"]) and pd.notna(sub_data.at[0, "chrcssrsb_css_sim4"]) and pd.notna(sub_data.at[0, "chrcssrsb_css_sim5"]):
						print("No Missing values")
						form_info.at["no_missing_cdssrsb", 'Variables'] = "1" # none is na
					else:
						form_info.at["no_missing_cdssrsb", 'Variables'] = "0"

			if name in ['cssrs_followup']:
			# month1
				if "chrcssrsfu_si1l" not in sub_data or  "chrcssrsfu_si2l" not in sub_data or "chrcssrsfu_si3l" not in sub_data or "chrcssrsfu_si4l" not in sub_data or "chrcssrsfu_si5l" not in sub_data or "chrcssrsfu_css_sim1" not in sub_data or "chrcssrsfu_css_sim2" not in sub_data or "chrcssrsfu_css_sim3" not in sub_data or "chrcssrsfu_css_sim4" not in sub_data or "chrcssrsfu_css_sim5" not in sub_data:
					print("Missing variables")
					form_info.at["no_missing_cdssrsfu", 'Variables'] = "0"
				else:
					if pd.notna(sub_data.at[0, "chrcssrsfu_si1l"]) and pd.notna(sub_data.at[0, "chrcssrsfu_si2l"]) and pd.notna(sub_data.at[0, "chrcssrsfu_si3l"]) and pd.notna(sub_data.at[0, "chrcssrsfu_si4l"]) and pd.notna(sub_data.at[0, "chrcssrsfu_si5l"]) and pd.notna(sub_data.at[0, "chrcssrsfu_css_sim1"]) and pd.notna(sub_data.at[0, "chrcssrsfu_css_sim2"]) and pd.notna(sub_data.at[0, "chrcssrsfu_css_sim3"]) and pd.notna(sub_data.at[0, "chrcssrsfu_css_sim4"]) and pd.notna(sub_data.at[0, "chrcssrsfu_css_sim5"]):
						print("No missing values")
						form_info.at["no_missing_cdssrsfu", 'Variables'] = "1"
					else:
						form_info.at["no_missing_cdssrsfu", 'Variables'] = "0"

			if name in ['premorbid_adjustment_scale']:
			# month1
				pas_6_11_q = 0
				pas_12_15_q = 0
				pas_16_18_q = 0
				pub_19_q = 0
				if "chrpas_pmod_child1" not in sub_data or  "chrpas_pmod_child1" not in sub_data or "chrpas_pmod_child2" not in sub_data or "chrpas_pmod_child3" not in sub_data or "chrpas_pmod_child4" not in sub_data:
					print("Missing variables")
					pas_6_11_q = 0
				else:
					if pd.isnull(sub_data.at[0, "chrpas_pmod_child1"]) or pd.isnull(sub_data.at[0, "chrpas_pmod_child2"]) or pd.isnull(sub_data.at[0, "chrpas_pmod_child3"]) or pd.isnull(sub_data.at[0, "chrpas_pmod_child4"]):
						pas_6_11_q = 0
					else:
						print("Complete variables")
						pas_6_11_q = 1
				# early adol
				if "chrpas_pmod_adol_early1" not in sub_data or "chrpas_pmod_adol_early2" not in sub_data or "chrpas_pmod_adol_early3" not in sub_data or "chrpas_pmod_adol_early4" not in sub_data or "chrpas_pmod_adol_early5" not in sub_data:
					pub_12_15_q = 0
				else:
					if pd.notna(sub_data.at[0, "chrpas_pmod_adol_early1"]) or pd.notna(sub_data.at[0, "chrpas_pmod_adol_early2"]) or pd.notna(sub_data.at[0, "chrpas_pmod_adol_early3"]) or pd.notna(sub_data.at[0, "chrpas_pmod_adol_early4"]) or pd.notna(sub_data.at[0, "chrpas_pmod_adol_early5"]) or pd.isnull(sub_data.at[0, "chrpas_pmod_child4"]):
						print("Not missing early adol data")
						pub_12_15_q = 1

				#late adol
				if "chrpas_pmod_adol_late1" not in sub_data or "chrpas_pmod_adol_late2" not in sub_data or "chrpas_pmod_adol_late3" not in sub_data or "chrpas_pmod_adol_late4" not in sub_data or "chrpas_pmod_adol_late5" not in sub_data:
					pub_16_18_q = 0
				else:
					if pd.notna(sub_data.at[0, "chrpas_pmod_adol_late1"]) or pd.notna(sub_data.at[0, "chrpas_pmod_adol_late2"]) or pd.notna(sub_data.at[0, "chrpas_pmod_adol_late3"]) or pd.notna(sub_data.at[0, "chrpas_pmod_adol_late4"]) or pd.notna(sub_data.at[0, "chrpas_pmod_adol_late5"]):
						print("Not missing late adol")
						pub_16_18_q = 1

#adult
				if "chrpas_pmod_adult1" not in sub_data or "chrpas_pmod_adult2" not in sub_data or ("chrpas_pmod_adult3v1" not in sub_data and "chrpas_pmod_adult3v3" not in sub_data):
					pub_19_q = 0
				else:
					if pd.isnull(sub_data.at[0, "chrpas_pmod_adult1"]) or pd.isnull(sub_data.at[0, "chrpas_pmod_adult2"]):
						print("Not missing adult data")
						pub_19_q = 1
					else:
						pub_19_q = 0
				# all questions
				form_info.at["no_missing_pubds", 'Variables'] = "0"
				if age < 15 and pas_6_11_q == 1:
					form_info.at["no_missing_pubds", 'Variables'] = "1"
				if age < 18 and pas_6_11_q == 1 and pub_12_15_q == 1:
					form_info.at["no_missing_pubds", 'Variables'] = "1"
				if age < 19 and pas_6_11_q == 1 and pub_12_15_q == 1 and pub_16_18_q == 1:
					form_info.at["no_missing_pubds", 'Variables'] = "1"
				if age > 19 and pas_6_11_q == 1 and pub_12_15_q == 1 and pub_16_18_q == 1 and pub_19_q == 1:
					form_info.at["no_missing_pubds", 'Variables'] = "1"
			

			## screening sofas
			if "chrsofas_premorbid" not in sub_data or  "chrsofas_currscore12mo" not in sub_data or "chrsofas_currscore" not in sub_data or "chrsofas_lowscore" not in sub_data:
				#print("Missing variables nay")
				form_info.at['no_missing_sofas', 'Variables'] = "0" # something missing
			else:
				if pd.notna(sub_data.at[0, "chrsofas_premorbid"]) and pd.notna(sub_data.at[0, "chrsofas_currscore12mo"]) and pd.notna(sub_data.at[0, "chrsofas_currscore"]) and pd.notna(sub_data.at[0, "chrsofas_lowscore"]):
					print("yay")
					form_info.at['no_missing_sofas', 'Variables'] = "1" # nothing missing or na
				else:
					form_info.at['no_missing_sofas', 'Variables'] = "0"
	
			#baseline sofas
			if "chrsofas_premorbid" not in sub_data or  "chrsofas_currscore12mo" not in sub_data or "chrsofas_currscore" not in sub_data or "chrsofas_lowscore" not in sub_data:
				#print("Missing variables nay")
				form_info.at['no_missing_sofas', 'Variables'] = "0" # something missing
			else:
				if pd.notna(sub_data.at[0, "chrsofas_premorbid"]) and pd.notna(sub_data.at[0, "chrsofas_currscore12mo"]) and pd.notna(sub_data.at[0, "chrsofas_currscore"]) and pd.notna(sub_data.at[0, "chrsofas_lowscore"]):
					print("yay")
					form_info.at['no_missing_sofas', 'Variables'] = "1" # nothing missing or na
				else:
					form_info.at['no_missing_sofas', 'Variables'] = "0"

			# informed consent run
			if "chric_consent_date" not in sub_data or  "chric_passive" not in sub_data or "chric_surveys" not in sub_data or "chric_actigraphy" not in sub_data:
				#print("Missing variables nay")
				form_info.at['no_missing_consent', 'Variables'] = "0" # something missing
			else:
				if pd.notna(sub_data.at[0, "chric_consent_date"]) and pd.notna(sub_data.at[0, "chric_passive"]) and pd.notna(sub_data.at[0, "chric_surveys"]) and pd.notna(sub_data.at[0, "chric_actigraphy"]):
					print("yay")
					form_info.at['no_missing_consent', 'Variables'] = "1" # nothing missing or na
				else:
					form_info.at['no_missing_consent', 'Variables'] = "0"

			#baseline global functioning role scale
			if "chrgfr_prompt2" not in sub_data or  "chrgfr_prompt3" not in sub_data or "chrgfr_prompt4" not in sub_data or "chrgfr_gf_primaryrole" not in sub_data or "chrgfr_gf_role_scole" not in sub_data or "chrgfr_gf_role_low" not in sub_data or "chrgfr_gf_role_high" not in sub_data:
				#print("Missing variables nay")
				form_info.at['no_missing_glob_func_role', 'Variables'] = "0" # something missing
			else:
				if pd.notna(sub_data.at[0, "chrgfr_prompt2"]) and pd.notna(sub_data.at[0, "chrgfr_prompt3"]) and pd.notna(sub_data.at[0, "chrgfr_prompt4"]) and pd.notna(sub_data.at[0, "chrgfr_gf_primaryrole"]) and pd.notna(sub_data.at[0, "chrgfr_gf_role_scole"]) and pd.notna(sub_data.at[0, "chrgfr_gf_role_low"]) and pd.notna(sub_data.at[0, "chrgfr_gf_role_high"]):
					print("yay")
					form_info.at['no_missing_glob_func_role', 'Variables'] = "1" # nothing missing or na
				else:
					form_info.at['no_missing_glob_func_role', 'Variables'] = "0"

			# baseline global functioning social
			if "chrgfs_overallsat" not in sub_data or  "chrgfs_gf_social_scale" not in sub_data or "chrgfs_gf_social_low" not in sub_data or "chrgfs_gf_social_high" not in sub_data:
				#print("Missing variables nay")
				form_info.at['no_missing_glob_func_social', 'Variables'] = "0" # something missing
			else:
				if pd.notna(sub_data.at[0, "chrgfs_overallsat"]) and pd.notna(sub_data.at[0, "chrgfs_gf_social_scale"]) and pd.notna(sub_data.at[0, "chrgfs_gf_social_low"]) and pd.notna(sub_data.at[0, "chrgfs_gf_social_high"]):
					print("yay")
					form_info.at['no_missing_glob_func_social', 'Variables'] = "1" # nothing missing or na
				else:
					form_info.at['no_missing_glob_func_social', 'Variables'] = "0"

			## Penn CNB baseline
			if "chrpenn_complete" not in sub_data or  "chrpenn_missing_1" not in sub_data:
				#print("Missing variables nay")
				form_info.at['no_missing_penncnb', 'Variables'] = "0" # something missing
			else:
				if pd.notna(sub_data.at[0, "chrpenn_complete"]) and pd.notna(sub_data.at[0, "chrpenn_missing_1"]):
					print("yay")
					form_info.at['no_missing_penncnb', 'Variables'] = "1" # nothing missing or na
				else:
					form_info.at['no_missing_penncnb', 'Variables'] = "0"

			## WRAT baseline
			if "chrpreiq_reading_task" not in sub_data or  "chrpreiq_total_raw" not in sub_data or "chrpreiq_standard_score" not in sub_data:
				#print("Missing variables nay")
				form_info.at['no_missing_wrat', 'Variables'] = "0" # something missing
			else:
				if pd.notna(sub_data.at[0, "chrpreiq_reading_task"]) and pd.notna(sub_data.at[0, "chrpreiq_total_raw"]) and pd.notna(sub_data.at[0, "chrpreiq_standard_score"]):
					print("yay")
					form_info.at['no_missing_wrat', 'Variables'] = "1" # nothing missing or na
				else:
					form_info.at['no_missing_wrat', 'Variables'] = "0"

			# WASII IQ baseline - need to add in variables that depend on which WASI used!
			if "chriq_assessment" not in sub_data or "chriq_vocab_raw" not in sub_data or "chriq_matrix_raw" not in sub_data or "chriq_fsiq" not in sub_data:
				#print("Missing variables nay")
				form_info.at['no_missing_wasiiq', 'Variables'] = "0" # something missing
			else:
				if pd.notna(sub_data.at[0, "chriq_assessment"]) and pd.notna(sub_data.at[0, "chriq_vocab_raw"]) and pd.notna(sub_data.at[0, "chriq_matrix_raw"]) and pd.notna(sub_data.at[0, "chriq_fsiq"]):
					print("yay")
					form_info.at['no_missing_wasiiq', 'Variables'] = "1" # nothing missing or na
				else:
					form_info.at['no_missing_wasiiq', 'Variables'] = "0"


			# creating a total score for oasis
			if name in ['oasis']:
				if sub_data.at[0, 'chroasis_oasis_1'] > -1 and sub_data.at[0, 'chroasis_oasis_2'] > -1 and sub_data.at[0, 'chroasis_oasis_3'] > -1 and sub_data.at[0, 'chroasis_oasis_4'] > -1 and sub_data.at[0, 'chroasis_oasis_5'] > -1: 					
					# creating list of values
					numbers = [sub_data.at[0, 'chroasis_oasis_1'], sub_data.at[0, 'chroasis_oasis_2'], sub_data.at[0, 'chroasis_oasis_3'], sub_data.at[0, 'chroasis_oasis_4'], sub_data.at[0, 'chroasis_oasis_5']]
					# adding values to get total score
					form_info.at["chroasis_oasis_total10", 'Variables'] = sum(numbers)				
				
				else:
					form_info.at["chroasis_oasis_total10", 'Variables'] = 999
				
			
			if name in ['daily_activity_and_saliva_sample_collection']:
				#with pd.option_context('display.max_rows', None, 'display.precision', 3,):
				#	print(sub_data.T)

				s_1 = s_2 = s_3 = s_4 = s_5 = s_6 = 0
				s_1_m2 = s_2_m2 = s_3_m2 = s_4_m2 = s_5_m2 = s_6_m2 = 0
				
				if "chrsaliva_vol1a" in sub_data and event == "2":
					s_1 = sub_data.at[0, "chrsaliva_vol1a"]
				if "chrsaliva_vol1b" in sub_data and event == "2":
					s_2 = sub_data.at[0, "chrsaliva_vol1b"]
				if "chrsaliva_vol2a" in sub_data and event == "2":
					s_3 = sub_data.at[0, "chrsaliva_vol2a"]
				if "chrsaliva_vol2b" in sub_data and event == "2":
					s_4 = sub_data.at[0, "chrsaliva_vol2b"]
				if "chrsaliva_vol3a" in sub_data and event == "2":
					s_5 = sub_data.at[0, "chrsaliva_vol3a"]
				if "chrsaliva_vol3b" in sub_data and event == "2":
					s_6 = sub_data.at[0, "chrsaliva_vol3b"]

				if "chrsaliva_vol1a" in sub_data and event == "4":
					s_1_m2 = sub_data.at[0, "chrsaliva_vol1a"]
				if "chrsaliva_vol1b" in sub_data and event == "4":
					s_2_m2 = sub_data.at[0, "chrsaliva_vol1b"]
				if "chrsaliva_vol2a" in sub_data and event == "4":
					s_3_m2 = sub_data.at[0, "chrsaliva_vol2a"]
				if "chrsaliva_vol2b" in sub_data and event == "4":
					s_4_m2 = sub_data.at[0, "chrsaliva_vol2b"]
				if "chrsaliva_vol3a" in sub_data and event == "4":
					s_5_m2 = sub_data.at[0, "chrsaliva_vol3a"]
				if "chrsaliva_vol3b" in sub_data and event == "4":
					s_6_m2 = sub_data.at[0, "chrsaliva_vol3b"]


				vials = [s_1, s_2, s_3, s_4, s_5, s_6]
				vials_m2 = [s_1_m2, s_2_m2, s_3_m2, s_4_m2, s_5_m2, s_6_m2]
				print(vials)		
				print(vials_m2)

				for x in range(len(vials)):
					if pd.isnull(vials[x]) or vials[x] == '-9' or vials[x] == '-3':
						vials[x] = 0

				for x in range(len(vials_m2)):
					if pd.isnull(vials_m2[x]) or vials_m2[x] == '-9' or vials_m2[x] == '-3':
						vials_m2[x] = 0

				print(vials)		
				print(vials_m2)
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
				if event == "2":
					form_info.at["number_saliva_vials", 'Variables'] = np.count_nonzero(vials)	
				if event == "4":
					form_info.at["number_saliva_vials", 'Variables'] = np.count_nonzero(vials_m2)	

				if np.count_nonzero(vials) > 4 and np.count_nonzero(vials_m2) > 4:
					print("Both complete")
					saliva_status = 3
				if np.count_nonzero(vials) > 4 and np.count_nonzero(vials_m2) < 5:
					print("Just baseline")
					saliva_status = 1
				if np.count_nonzero(vials) < 5 and np.count_nonzero(vials_m2) > 4:
					print("Just month 2")
					saliva_status = 2
				if np.count_nonzero(vials) < 5 and np.count_nonzero(vials_m2) < 5:
					print("Incomplete")
					saliva_status = 0

			#if name in ['current_health_status']: 
				#with pd.option_context('display.max_rows', None, 'display.precision', 3,):
				#	print(sub_data.T)

			if name in ['current_health_status']: 
				#with pd.option_context('display.max_rows', None, 'display.precision', 3,):
				#	print(sub_data.T)
				print("Event: " + str(event) + " Time: " + str(sub_data.at[0, "chrchs_ate"]))
				if event == "2":
					date_ate_bl = sub_data.at[0, "chrchs_ate"]
					print(str(date_ate_bl))	
 				
					if pd.notna(date_ate_bl) and date_ate_bl != -3 and date_ate_bl != -9:
						date_ate_bl = datetime.strptime(date_ate_bl, "%d/%m/%Y %I:%M:%S %p")
						print(str(date_ate_bl))

				if event == "4" and pd.notna(sub_data.at[0, "chrchs_ate"]) and sub_data.at[0, "chrchs_ate"] != -3 and sub_data.at[0, "chrchs_ate"] != -9:
					date_ate_m2 = sub_data.at[0, "chrchs_ate"]
					print(str(date_ate_m2))
					date_ate_m2 = datetime.strptime(date_ate_m2, "%d/%m/%Y %I:%M:%S %p")
					print(str(date_ate_m2))

			if name in ['blood_sample_preanalytic_quality_assurance']: 
				if event == "2" or event == "4":
					drawdate = sub_data.at[0, "chrblood_labdate"]
					chrblood_wbfrztime = sub_data.at[0, "chrblood_wbfrztime"]
					chrblood_bcfrztime = sub_data.at[0, "chrblood_bcfrztime"]
					chrblood_serumfrztime = sub_data.at[0, "chrblood_serumfrztime"]
					chrblood_plfrztime = sub_data.at[0, "chrblood_plfrztime"]

					if pd.notna(drawdate) and drawdate != "-3" and drawdate != "-9":
						print("Drawdate: " + str(drawdate))
						if event == '2':
							date_drawn_bl = datetime.strptime(drawdate, "%d/%m/%Y %I:%M:%S %p")
						else:
							date_drawn_m2 = datetime.strptime(drawdate, "%d/%m/%Y %I:%M:%S %p")
						drawdate = drawdate.split(" ", 1 )[1]			
						drawdate = datetime.strptime(drawdate, "%I:%M:%S %p")
						
						if pd.notna(chrblood_wbfrztime) and chrblood_wbfrztime != "-3" and chrblood_wbfrztime != "-9":
							chrblood_wbfrztime = chrblood_wbfrztime.split(" ", 1)[1]
							print(str(chrblood_wbfrztime))
							chrblood_wbfrztime =  datetime.strptime(chrblood_wbfrztime, "%I:%M:%S %p")
							chrblood_wholeblood_freeze = ((chrblood_wbfrztime - drawdate).total_seconds()) / 60
							print("Whole blood processing time: " + str(chrblood_wholeblood_freeze))
							#form_info.at["chrblood_wholeblood_freeze", 'Variables'] =  chrblood_wholeblood_freeze
						else:
							print("Missing whole blood freeze time")
							#form_info.at["chrblood_wholeblood_freeze", 'Variables'] = np.nan

						if pd.notna(chrblood_bcfrztime) and chrblood_bcfrztime != "-3" and chrblood_wbfrztime != "-9":
							chrblood_bcfrztime = chrblood_bcfrztime.split(" ", 1)[1]
							print("Buffy freeze time: " + str(chrblood_bcfrztime))
							chrblood_bcfrztime = datetime.strptime(chrblood_bcfrztime, "%I:%M:%S %p")
							chrblood_buffy_freeze = ((chrblood_bcfrztime - drawdate).total_seconds()) / 60
							print('Buffy processing time: ' + str(chrblood_buffy_freeze))
							#form_info.at["chrblood_buffy_freeze", 'Variables'] = chrblood_buffy_freeze 

							buffy = 0

							if chrblood_buffy_freeze < 60 and chrblood_buffy_freeze > 0:
								buffy = 1
							if chrblood_buffy_freeze > 60 and chrblood_buffy_freeze < 121:
								buffy = 2
							if chrblood_buffy_freeze > 120:
								buffy = 3

							print(" Label: " + str(buffy) + "\n")
							#form_info.at["buffy_time", 'Variables'] = buffy
						else:
							print("Missing buffy blood freeze time")
							#form_info.at["chrblood_buffy_freeze", 'Variables'] = np.nan

						if pd.notna(chrblood_serumfrztime) and chrblood_serumfrztime != "-3" and chrblood_serumfrztime != "-9":
							chrblood_serumfrztime = chrblood_serumfrztime.split(" ", 1)[1]
							print("Serum freeze time: " + str(chrblood_serumfrztime))
							chrblood_serumfrztime = datetime.strptime(chrblood_serumfrztime, "%I:%M:%S %p")
							chrblood_serum_freeze = ((chrblood_serumfrztime - drawdate).total_seconds()) / 60
							print('Serum processing time: ' + str(chrblood_serum_freeze))
							#form_info.at["chrblood_serum_freeze", 'Variables'] = chrblood_serum_freeze 
						else:
							print("Missing serum blood freeze time")
							#form_info.at["chrblood_serum_freeze", 'Variables'] = np.nan

						if pd.notna(chrblood_plfrztime) and chrblood_plfrztime != "-3" and chrblood_plfrztime != "-9":
							chrblood_plfrztime = chrblood_plfrztime.split(" ", 1)[1]
							print("Plasma freeze time: " + str(chrblood_plfrztime))
							chrblood_plfrztime = datetime.strptime(chrblood_plfrztime, "%I:%M:%S %p")
							chrblood_plasma_freeze = ((chrblood_plfrztime - drawdate).total_seconds()) / 60
							print('Plasma processing time: ' + str(chrblood_plasma_freeze))
							#form_info.at["chrblood_plasma_freeze", 'Variables'] = chrblood_serum_freeze 
						else:
							print("Missing plasma blood freeze time")
							#form_info.at["chrblood_plasma_freeze", 'Variables'] = np.nan	
					

				##
				
				
					
				bc_1 = wb_1 = wb_2 = wb_3 = wb_4 = wb_5 = 0
				sr_1 = sr_2 = sr_3 = sr_4 = sr_5 = 0
				pl_1 = pl_2 = pl_3 = pl_4 = pl_5 = pl_6 = pl_7 = 0
				bc_1_m2 = wb_1_m2 = wb_2_m2 = wb_3_m2 = wb_4_m2 = wb_5_m2 = 0
				sr_1_m2 = sr_2_m2 = sr_3_m2 = sr_4_m2 = sr_5_m2 = 0
				pl_1_m2 = pl_2_m2 = pl_3_m2 = pl_4_m2 = pl_5_m2 = pl_6_m2 = pl_7_m2 = 0

				if "chrblood_bc1vol" in sub_data and event == "2":
					bc_1 = sub_data.at[0, "chrblood_bc1vol"]

				if "chrblood_wb1vol" in sub_data and event == "2":
					wb_1 = sub_data.at[0, "chrblood_wb1vol"]
				if "chrblood_wb2vol" in sub_data and event == "2":
					wb_2 = sub_data.at[0, "chrblood_wb2vol"]
				if "chrblood_wb3vol" in sub_data and event == "2":
					wb_3 = sub_data.at[0, "chrblood_wb3vol"]
				if "chrblood_wb4vol" in sub_data and event == "2":
					wb_4 = sub_data.at[0, "chrblood_wb4vol"]
				if "chrblood_wb5vol" in sub_data and event == "2":
					wb_5 = sub_data.at[0, "chrblood_wb5vol"]

				if "chrblood_se1vol" in sub_data and event == "2":
					sr_1 = sub_data.at[0, "chrblood_se1vol"]
				if "chrblood_se2vol" in sub_data and event == "2":
					sr_2 = sub_data.at[0, "chrblood_se2vol"]
				if "chrblood_se3vol" in sub_data and event == "2":
					sr_3 = sub_data.at[0, "chrblood_se3vol"]
				if "chrblood_se4vol" in sub_data and event == "2":
					sr_4 = sub_data.at[0, "chrblood_se4vol"]
				if "chrblood_se5vol" in sub_data and event == "2":
					sr_5 = sub_data.at[0, "chrblood_se5vol"]

				if "chrblood_pl1vol" in sub_data and event == "2":
					pl_1 = sub_data.at[0, "chrblood_pl1vol"]
				if "chrblood_pl2vol" in sub_data and event == "2":
					pl_2 = sub_data.at[0, "chrblood_pl2vol"]
				if "chrblood_pl3vol" in sub_data and event == "2":
					pl_3 = sub_data.at[0, "chrblood_pl3vol"]
				if "chrblood_pl4vol" in sub_data and event == "2":
					pl_4 = sub_data.at[0, "chrblood_pl4vol"]
				if "chrblood_pl5vol" in sub_data and event == "2":
					pl_5 = sub_data.at[0, "chrblood_pl5vol"]
				if "chrblood_pl6vol" in sub_data and event == "2":
					pl_6 = sub_data.at[0, "chrblood_pl6vol"]
				if "chrblood_pl7vol" in sub_data and event == "2":
					pl_7 = sub_data.at[0, "chrblood_pl7vol"]


				if "chrblood_bc1vol" in sub_data and event == "4":
					bc_1_m2 = sub_data.at[0, "chrblood_bc1vol"]

				if "chrblood_wb1vol" in sub_data and event == "4":
					wb_1_m2 = sub_data.at[0, "chrblood_wb1vol"]
				if "chrblood_wb2vol" in sub_data and event == "4":
					wb_2_m2 = sub_data.at[0, "chrblood_wb2vol"]
				if "chrblood_wb3vol" in sub_data and event == "4":
					wb_3_m2 = sub_data.at[0, "chrblood_wb3vol"]
				if "chrblood_wb4vol" in sub_data and event == "4":
					wb_4_m2 = sub_data.at[0, "chrblood_wb4vol"]
				if "chrblood_wb5vol" in sub_data and event == "4":
					wb_5_m2 = sub_data.at[0, "chrblood_wb5vol"]

				if "chrblood_se1vol" in sub_data and event == "4":
					sr_1_m2 = sub_data.at[0, "chrblood_se1vol"]
				if "chrblood_se2vol" in sub_data and event == "4":
					sr_2_m2 = sub_data.at[0, "chrblood_se2vol"]
				if "chrblood_se3vol" in sub_data and event == "4":
					sr_3_m2 = sub_data.at[0, "chrblood_se3vol"]
				if "chrblood_se4vol" in sub_data and event == "4":
					sr_4_m2 = sub_data.at[0, "chrblood_se4vol"]
				if "chrblood_se5vol" in sub_data and event == "4":
					sr_5_m2 = sub_data.at[0, "chrblood_se5vol"]

				if "chrblood_pl1vol" in sub_data and event == "4":
					pl_1_m2 = sub_data.at[0, "chrblood_pl1vol"]
				if "chrblood_pl2vol" in sub_data and event == "4":
					pl_2_m2 = sub_data.at[0, "chrblood_pl2vol"]
				if "chrblood_pl3vol" in sub_data and event == "4":
					pl_3_m2 = sub_data.at[0, "chrblood_pl3vol"]
				if "chrblood_pl4vol" in sub_data and event == "4":
					pl_4_m2 = sub_data.at[0, "chrblood_pl4vol"]
				if "chrblood_pl5vol" in sub_data and event == "4":
					pl_5_m2 = sub_data.at[0, "chrblood_pl5vol"]
				if "chrblood_pl6vol" in sub_data and event == "4":
					pl_6_m2 = sub_data.at[0, "chrblood_pl6vol"]
				if "chrblood_pl7vol" in sub_data and event == "4":
					pl_7_m2 = sub_data.at[0, "chrblood_pl7vol"]


				vials = [wb_1, wb_2, wb_3, wb_4, wb_5, sr_1, sr_2, sr_3, sr_4, sr_5, pl_1, pl_2, pl_3, pl_4, pl_5, pl_6, pl_7, bc_1] #17
				vials_m2 = [wb_1_m2, wb_2_m2, wb_3_m2, wb_4_m2, wb_5_m2, sr_1_m2, sr_2_m2, sr_3_m2, sr_4_m2, sr_5_m2, pl_1_m2, pl_2_m2, pl_3_m2, pl_4_m2, pl_5_m2, pl_6_m2, pl_7_m2, bc_1_m2]
				print(vials)
				print(vials_m2)

				for x in range(len(vials)):
					if pd.isnull(vials[x]) or vials[x] == '-9' or vials[x] == '-3':
						vials[x] = 0

				for x in range(len(vials_m2)):
					if pd.isnull(vials_m2[x]) or vials_m2[x] == '-9' or vials_m2[x] == '-3':
						vials_m2[x] = 0

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
				if event == "2":
					form_info.at["number_blood_vials", 'Variables'] = np.count_nonzero(vials)	
				if event == "4":
					form_info.at["number_blood_vials", 'Variables'] = np.count_nonzero(vials_m2)	

				if np.count_nonzero(vials) > 12 and np.count_nonzero(vials_m2) > 12:
					print("Both complete")
					blood_status = 3
				if np.count_nonzero(vials) > 12 and np.count_nonzero(vials_m2) < 13:
					print("Just baseline")
					blood_status = 1
				if np.count_nonzero(vials) < 13 and np.count_nonzero(vials_m2) > 12:
					print("Just month 2")
					blood_status = 2
				if np.count_nonzero(vials) < 13 and np.count_nonzero(vials_m2) < 13:
					print("Incomplete")
					blood_status = 0




					#plasma_time = sub_data.loc[0, "chrblood_plfrztime"]
					#print("Plasma freeze time: " + str(plasma_time))
					#form_info.at["plasma_time", 'Variables'] = plasma_time
					#print(form_info)
					# usually nan
					#plasma_time = int(plasma_time)

				#if plasma_time < 60:
				#	plasma = 1
				#if plasma_time > 60 and plasma_time < 121:
				#	plasma = 2
				#if plasma_time > 120:
				#	plasma = 3
				#if plasma_time == -3 or plasma_time == -9 or pd.notna(plasma_time):
				#	plasma = 0

			#print ("Plasma processing time: " + str(plasma_time) + " Label: " + str(plasma))
			#form_info.at["plasma_time", baseline] = plasma

				# EDTA plasma sample
				print("Plasma volumes:")
				#pl_1 = sub_data.at[0, "chrblood_pl1vol"]
				#print("Pl1: " + str(pl_1))
				#pl_2 = sub_data.at[0, "chrblood_pl2vol"]
				#pl_3 = sub_data.at[0, "chrblood_pl3vol"]
				#pl_4 = sub_data.at[0, "chrblood_pl4vol"]
				#pl_5 = sub_data.at[0, "chrblood_pl5vol"]
				#pl_6 = sub_data.at[0, "chrblood_pl6vol"]

				#num_vials = np.nansum([pl_1 + pl_2 + pl_3 + pl_4 + pl_5 + pl_6])
				#print("Number of vials (max 6): " + str(num_vials))
				#form_info.at["num_plasma_vials", 'Variables'] = num_vials
				
				print("Fasting Time")
				if event == "2":
					if pd.isna(date_drawn_bl) or pd.isna(date_ate_bl):
						print("Fasting time basline not complete")
					else:
						print(str(date_drawn_bl))
						print(str(date_ate_bl))
						print(str((((date_drawn_bl - date_ate_bl).total_seconds()) / 60) / 60))
						form_info.at["time_fasting", 'Variables'] = round((((date_drawn_bl - date_ate_bl).total_seconds()) / 60) / 60, 1)
				
				if event == "4":
					if pd.isna(date_drawn_m2) or pd.isna(date_ate_m2):
						print("Fasting time Month 2 not complete")
					else:
						print(str(date_drawn_m2))
						print(str(date_ate_m2))
						form_info.at["time_fasting", 'Variables'] = round((((date_drawn_m2 - date_ate_m2).total_seconds()) / 60) /60, 1)
				#print(form_info)


			#### setting up combined forms
			# moved this down so that the calculated variables would be included
			if event == '1':
				#print("Saving screening variables")
				screening_tracker["Screening_{0}".format(name)] = form_info
			
			if event == '2':
				#print("Saving baseline variables")
				baseline_tracker["Baseline_{0}".format(name)] = form_info

			if event == '3':
				#print("Saving month1 variables")
				month1_tracker["Month1_{0}".format(name)] = form_info

			if event == '4':
				#print("Saving month2 variables")
				month2_tracker["Month2_{0}".format(name)] = form_info

			if event == '5':
				#print("Saving month3 variables")
				month3_tracker["Month3_{0}".format(name)] = form_info

			if event == '6':
				#print("Saving month4 variables")
				month4_tracker["Month4_{0}".format(name)] = form_info

			if event == '7':
				#print("Saving month4 variables")
				month5_tracker["Month5_{0}".format(name)] = form_info

			if event == '8':
				month6_tracker["Month6_{0}".format(name)] = form_info

			if event == '9':
				month7_tracker["Month7_{0}".format(name)] = form_info

			if event == '10':
				month8_tracker["Month8_{0}".format(name)] = form_info

			if event == '98':
				conversion_tracker["Conversion_{0}".format(name)] = form_info
	
			# transposing
			#print(form_info)
			form_info = form_info.transpose()


			names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
			dpdash_main = pd.DataFrame(columns = names_dash)
			dpdash_main.at[event, 'subjectid'] = id
			dpdash_main.at[event, 'site'] = site
			dpdash_main.at[event, 'file_updated'] = today

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
				#print("Time between interview and entry date: ")
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
		
		if pd.isna(last_day):
			last_day = 1

		if not pd.isna(last_day) and int(last_day) > 300:
			print("ALERT: ERROR WIITH DATE IN FORM - Last day too high")
			last_day = 300

		final_csv = pd.concat(form_tracker, axis=0, ignore_index=True)
		#print(name)
		#removing rows with no data
		#final_csv = final_csv[final_csv.Percentage != 0]
		#print("FINAL CSV TO BE EXPORTED:\n", final_csv.T)
	
		# Saving to a csv based on ID and event
		final_csv.to_csv(output1 + site+"-"+id+"-form_"+name+'-day1to'+str(last_day)+".csv", sep=',', index = False, header=True)


else:
	print("No file for form: " + name)

####
names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
dpdash_main = pd.DataFrame(columns = names_dash)
dpdash_main.at[0, 'subjectid'] = id
dpdash_main.at[0, 'site'] = site
dpdash_main.at[0, 'file_updated'] = today
dpdash_main.at[0, 'day'] = 1
dpdash_main.at[0, 'mtime'] = consent

name = 'chart_statuses'
form_info = pd.DataFrame()
form_info.at["Percentage", 'Variables'] = 100


print("Cognition status")
cognition_status = 0
if bl_preiq == 1 and bl_wasi == 1 and bl_penn == 1:
	print("baseline complete")
	bl_cog = 1
else:
	bl_cog = 0
if bl_cog == 1 and m2_penn == 1:
	print("Both baseline and month 2 cog")
	cognition_status = 3
if bl_cog == 1 and m2_penn == 0:
	print("Only baseline cog")
	cognition_status = 1
if bl_cog == 0 and m2_penn == 1:
	print("ONly month 2 cog")
	cognition_status = 2


# clinical status
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
	if bl_clin == 1 and m2_clin == 0:
		print("Only baseline clin")
		clinical_status = 1
	if bl_clin == 0 and m2_clin == 1:
		print("ONly month 2 cog")
		clinical_status = 2
	if bl_clin == 0 and m2_clin == 0:
		print("Not completed")
		clinical_status = 0

form_info.at["cognition_status", 'Variables'] = cognition_status
form_info.at["clinical_status", 'Variables'] = clinical_status
form_info.at["blood_status", 'Variables'] = blood_status
form_info.at["saliva_status", 'Variables'] = saliva_status
form_info.at["included_excluded", 'Variables'] = included
form_info.at["interview_age", 'Variables'] = age_chart
form_info.at["chrdemo_sexassigned", 'Variables'] = sex_chart

form_info = form_info.transpose()

dpdash_main = dpdash_main.reset_index(drop=True)
form_info = form_info.reset_index(drop=True)
print("STATUS: " + str(status))
#print(dpdash_main)
#print(form_info)

frames = [dpdash_main, form_info]
dpdash_full = pd.concat(frames, axis=1)
print(dpdash_full.T)
dpdash_full.to_csv(output1 + site+"-"+str(id)+"-form_"+str(name)+"-day1to1.csv", sep=',', index = False, header=True)


### putting together the combined csvs	
print("Printing percentage")

if len(screening_tracker) > 0:
	screening = pd.concat(screening_tracker, axis=0)
	#print(screening)
	screening.to_csv(output1 + site+"-"+id+"-screening.csv", sep=',', index = True, header=True)

if len(baseline_tracker) > 0:
	baseline = pd.concat(baseline_tracker, axis=0)
	#print(baseline)
	baseline.to_csv(output1 + site+"-"+id+"-baseline.csv", sep=',', index = True, header=True)

if len(conversion_tracker) > 0:
	conversion = pd.concat(conversion_tracker, axis=0)
	#print(baseline)
	conversion.to_csv(output1 + site+"-"+id+"-conversion.csv", sep=',', index = True, header=True)


# all the other months
for vi in ["1", "2", "3", "4", "5", "6", "7", "8"]:
	print(vi)
	tracker_name = vars()['month' + str(vi) + '_tracker']
	#print(tracker_name)

	if len(tracker_name) > 0:
		concat_csv = pd.concat(tracker_name, axis=0)
		#print(concat_csv)
		concat_csv.to_csv(output1 + site+"-"+id+"-month" + str(vi) +".csv", sep=',', index = True, header=True)


#print(percentage_form.T)
percentage_form.to_csv(output1 + site+"-"+id+"-percentage.csv", sep=',', index = True, header=True)



