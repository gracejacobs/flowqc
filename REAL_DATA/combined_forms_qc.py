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

def createList(r1, r2):
    return list(range(r1, r2+1))

# getting today's date
today = date.today()
today = today.strftime("%Y-%m-%d")

# output folder
#output1 = "/data/predict/data_from_ndaformqc/"
output1 = "/data/predict1/data_from_nda/formqc/"

# list of sites for site-specific combined files
site_list = ["BI", "CA", "CM", "GA", "KC", "SD", "SF", "SI", "HA", "YA", "LA", "WU", "PI", "PA", "PV", "OR", "NN", "IR", "NL", "NN", "NC", "TE", "MT", "LS"]

# list of ids to include
ids = pd.read_csv("/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/pronet_sub_list_chr.txt", sep= "\n", index_col = False, header = None)

id_list = ids.iloc[:, 0].tolist()
#id_list = ids.iloc[1:5, 0].tolist()
#id_list = ["YA05271"]

# Printing out IDs
print("ID List: ")
for i in id_list:
	print(i)

id_tracker = {}
id_baseline_tracker = {}
id_month1_tracker = {}
id_month2_tracker = {}
id_month3_tracker = {}
id_month4_tracker = {}
id_month5_tracker = {}
id_month6_tracker = {}
id_month7_tracker = {}
id_month8_tracker = {}
id_conversion_tracker = {}

print("\nCombining all measures for screening, baseline, and month 1-4 visits: ")

# for each ID going to pull out the variables
# then going to append them to each other
for id in id_list:
	print("\nID: " + id)
	last_date = id.split(" ")[0]
	print(last_date)
	id = id.split(" ")[1]
	print(id)
	site = id[0:2] #taking first part of ID
	print("Site: ", site)

	sub_data = "/data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)

	#print(sub_data)
	with open(sub_data, 'r') as f:
		sub_json = json.load(f)

	sub_data_all = pd.DataFrame.from_dict(sub_json, orient="columns")
	#replacing empty cells with NaN
	sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)
	sub_data_all.dropna(axis=1, how='all', inplace=True)

	print(sub_data_all)
	
	all_events = sub_data_all['redcap_event_name'].unique()
	screening = [i for i in all_events if i.startswith('screening_')][0]

	baseline = [i for i in all_events if i.startswith('baseline_')]
	if baseline == []:
		baseline = []
	else:
		baseline = baseline[0]

	conversion = [i for i in all_events if i.startswith('conversion_')]
	if conversion == []:
		conversion = []
	else:
		conversion = conversion[0]

	for vi in ["1", "2", "3", "4", "5", "6", "7", "8"]:
		#visit_data = vars()['sub_data_month' + str(vi)]
		#name = ('sub_data_month' + str(vi))
		#print(visit_data)
		visit_data = pd.DataFrame()

		evlist = [i for i in all_events if i.startswith('month_' + str(vi) + "_")]
		if evlist == []:
			name = []
		else:
			name = evlist[0]
			print(str(name))

		if name != []:
			print("Subsetting data for: " + str(name))
			visit_data = sub_data_all[sub_data_all['redcap_event_name'].isin([name])]
			globals()['sub_data_month'+str(vi)] = visit_data.reset_index(drop=True)
			#print(visit_data)
		else:
			globals()['sub_data_month'+str(vi)] = pd.DataFrame()

	print("Printing month 1 data")
	print(sub_data_month1)

	# setting up screening
	sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([screening])]
	sub_data = sub_data.reset_index(drop=True)
	#print(sub_data)
	
	# setting up baseline
	if baseline != []:
		sub_data_baseline = sub_data_all[sub_data_all['redcap_event_name'].isin([baseline])]
		sub_data_baseline = sub_data_baseline.reset_index(drop=True)
	else:
		sub_data_baseline = pd.DataFrame()

	# setting up conversion
	if conversion != []:
		sub_data_conversion = sub_data_all[sub_data_all['redcap_event_name'].isin([conversion])]
		sub_data_conversion = sub_data_conversion.reset_index(drop=True)
	else:
		sub_data_conversion = pd.DataFrame()

	# Adding age variable
	age = 0
	age_months = 0
	if 'chrdemo_age_mos_chr' in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrdemo_age_mos_chr"]):
		age_months = sub_data_baseline.at[0, "chrdemo_age_mos_chr"]
		print(str(age_months))

	if 'chrdemo_age_mos_hc' in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrdemo_age_mos_hc"]):
		age_months = sub_data_baseline.at[0, "chrdemo_age_mos_hc"]
		print(str(age_months))
		
	if 'chrdemo_entry_date' in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrdemo_entry_date"]):
		int_date = sub_data_baseline.at[0, "chrdemo_entry_date"]
		months_bet = (days_between(sub_data.at[0, "chric_consent_date"], int_date))/30

		age_months = int(age_months)
		age = round(age_months/12, 1)

		age_adj = age_months - months_bet
		age_adj = round(age_adj/12, 1)

		print("Adjusted age: "  + str(age_adj))
		sub_data_baseline.at[0, "consent_age"] = age_adj
		sub_data_baseline.at[0, 'interview_age'] = age
		
	print("Age: " + str(age))	


	## adding inclusion/exclusion criteria
	sub_data.at[0, "included_excluded"] = np.nan

	if 'chrcrit_included' in sub_data and pd.notna(sub_data.at[0, "chrcrit_included"]):
		if sub_data.loc[0, "chrcrit_included"] == "1":
			sub_data.at[0, "included_excluded"] = 1
		else:
			sub_data.at[0, "included_excluded"] = 0
	#if "chrcrit_excluded" in sub_data and pd.notna(sub_data.loc[0, "chrcrit_excluded"]) and sub_data.loc[0, "chrcrit_excluded"] == "1":
	#	sub_data.at[0, "included_excluded"] = 0

	# making a yes/no GUID form for whether there is a GUID or pseudoguid
	sub_data.at[0, "guid_available"] = np.nan
	sub_data.at[0, "pseudoguid_available"] = np.nan

	if "chrguid_guid" in sub_data and pd.notna(sub_data.loc[0, "chrguid_guid"]):
		sub_data.at[0, "guid_available"] = 1
	else:
		sub_data.at[0, "guid_available"] = 0
	# pseudoguid
	if "chrguid_pseudoguid" in sub_data and pd.notna(sub_data.loc[0, "chrguid_pseudoguid"]):
		sub_data.at[0, "pseudoguid_available"] = 1
	else:
		sub_data.at[0, "pseudoguid_available"] = 0

	print("Creating time_fasting variable")
	date_drawn_bl = np.nan
	date_ate_bl = np.nan
	date_drawn_m2 = np.nan
	date_ate_m2 = np.nan
	if "chrblood_drawdate" in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrblood_drawdate"]) and sub_data_baseline.at[0, "chrblood_drawdate"] != '-3' and sub_data_baseline.at[0, "chrblood_drawdate"] != '-9' and sub_data_baseline.at[0, "chrblood_drawdate"] != "1903-03-03":
			date_drawn_bl = sub_data_baseline.at[0, "chrblood_drawdate"]
			date_drawn_bl = datetime.strptime(date_drawn_bl, "%Y-%m-%d %H:%M")
			print(str(date_drawn_bl))
	if "chrblood_drawdate" in sub_data_month2 and pd.notna(sub_data_month2.at[0, "chrblood_drawdate"]) and sub_data_month2.at[0, "chrblood_drawdate"] != '-3' and sub_data_month2.at[0, "chrblood_drawdate"] != '-9' and sub_data_month2.at[0, "chrblood_drawdate"] != "1903-03-03":
			date_drawn_m2 = sub_data_month2.at[0, "chrblood_drawdate"]
			date_drawn_m2 = datetime.strptime(date_drawn_m2, "%Y-%m-%d %H:%M")
			print(str(date_drawn_m2))
	if "chrchs_ate" in sub_data_baseline and pd.notna(sub_data_baseline.at[0, "chrchs_ate"]) and sub_data_baseline.at[0, "chrchs_ate"] != '-3' and sub_data_baseline.at[0, "chrchs_ate"] != '-9' and sub_data_baseline.at[0, "chrchs_ate"] != "1903-03-03":
		#with pd.option_context('display.max_rows', None, 'display.precision', 3,):
			#print(form_info_2)
			date_ate_bl = sub_data_baseline.at[0, "chrchs_ate"]
			date_ate_bl = datetime.strptime(date_ate_bl, "%Y-%m-%d %H:%M")
			print(str(date_ate_bl))
	if "chrchs_ate" in sub_data_month2 and pd.notna(sub_data_month2.at[0, "chrchs_ate"]) and sub_data_month2.at[0, "chrchs_ate"] != '-3' and sub_data_month2.at[0, "chrchs_ate"] != '-9' and sub_data_month2.at[0, "chrchs_ate"] != "1903-03-03":
			date_ate_m2 = sub_data_month2.at[0, "chrchs_ate"]
			date_ate_m2 = datetime.strptime(date_ate_m2, "%Y-%m-%d %H:%M")
			print(str(date_ate_m2))

	if pd.isna(date_drawn_bl) or pd.isna(date_ate_bl):
		print("Basline not complete")
	else:
		print(str((((date_drawn_bl - date_ate_bl).total_seconds()) / 60) / 60))
		sub_data_baseline.at[0, "time_fasting"] = round((((date_drawn_bl - date_ate_bl).total_seconds()) / 60) / 60, 1)
	if pd.isna(date_drawn_m2) or pd.isna(date_ate_m2):
		print("Month 2 not complete")
	else:
		print(str((((date_drawn_m2 - date_ate_m2).total_seconds()) / 60) / 60))
		sub_data_month2.at[0, "time_fasting"] = round((((date_drawn_m2 - date_ate_m2).total_seconds()) / 60) / 60, 1)

	print('blood_sample_preanalytic_quality_assurance')

		 #days_between(sub_data.at[0, "chric_consent_date"], today)
		#chrblood_drawdate
		#chrchs_ate - current health status	


	wb_1_m2 = wb_2_m2 = wb_3_m2 = 0
	sr_1_m2 = sr_2_m2 = sr_3_m2 = 0
	pl_1_m2 = pl_2_m2 = pl_3_m2 = pl_4_m2 = pl_5_m2 = pl_6_m2 = 0
	bc_1_m2 = 0

	wb_1 = wb_2 = wb_3 = 0
	sr_1 = sr_2 = sr_3 = 0
	pl_1 = pl_2 = pl_3 = pl_4 = pl_5 = pl_6 = 0
	bc_1 = 0

	if "chrblood_bc1vol" in sub_data_baseline:
		bc_1 = sub_data_baseline.at[0, "chrblood_bc1vol"]
	if "chrblood_bc1vol" in sub_data_month2:
		bc_1_m2 = sub_data_month2.at[0, "chrblood_bc1vol"]
	if "chrblood_wb1vol" in sub_data_baseline:
		wb_1 = sub_data_baseline.at[0, "chrblood_wb1vol"]
	if "chrblood_wb1vol" in sub_data_month2:
		wb_1_m2 = sub_data_month2.at[0, "chrblood_wb1vol"]
	if "chrblood_wb2vol" in sub_data_baseline:
		wb_2 = sub_data_baseline.at[0, "chrblood_wb2vol"]
	if "chrblood_wb2vol" in sub_data_month2:
		wb_2_m2 = sub_data_month2.at[0, "chrblood_wb2vol"]
	if "chrblood_wb3vol" in sub_data_baseline:	
		wb_3 = sub_data_baseline.at[0, "chrblood_wb3vol"]
	if "chrblood_wb3vol" in sub_data_month2:
		wb_3_m2 = sub_data_month2.at[0, "chrblood_wb3vol"]
	if "chrblood_se1vol" in sub_data_baseline:
		sr_1 = sub_data_baseline.at[0, "chrblood_se1vol"]
	if "chrblood_se1vol" in sub_data_month2:
		sr_1_m2 = sub_data_month2.at[0, "chrblood_se1vol"]
	if "chrblood_se2vol" in sub_data_baseline:
		sr_2 = sub_data_baseline.at[0, "chrblood_se2vol"]
	if "chrblood_se2vol" in sub_data_month2:
		sr_2_m2 = sub_data_month2.at[0, "chrblood_se2vol"]
	if "chrblood_se3vol" in sub_data_baseline:
		sr_3 = sub_data_baseline.at[0, "chrblood_se3vol"]
	if "chrblood_se3vol" in sub_data_month2:
		sr_3_m2 = sub_data_month2.at[0, "chrblood_se3vol"]
	if "chrblood_pl1vol" in sub_data_baseline:
		pl_1 = sub_data_baseline.at[0, "chrblood_pl1vol"]
	if "chrblood_pl1vol" in sub_data_month2:
		pl_1_m2 = sub_data_month2.at[0, "chrblood_pl1vol"]
	if "chrblood_pl2vol" in sub_data_baseline:
		pl_2 = sub_data_baseline.at[0, "chrblood_pl2vol"]
	if "chrblood_pl2vol" in sub_data_month2:
		pl_2_m2 = sub_data_month2.at[0, "chrblood_pl2vol"]
	if "chrblood_pl3vol" in sub_data_baseline:
		pl_3 = sub_data_baseline.at[0, "chrblood_pl3vol"]
	if "chrblood_pl3vol" in sub_data_month2:
		pl_3_m2 = sub_data_month2.at[0, "chrblood_pl3vol"]
	if "chrblood_pl4vol" in sub_data_baseline:
		pl_4 = sub_data_baseline.at[0, "chrblood_pl4vol"]
	if "chrblood_pl4vol" in sub_data_month2:
		pl_4_m2 = sub_data_month2.at[0, "chrblood_pl4vol"]
	if "chrblood_pl5vol" in sub_data_baseline:
		pl_5 = sub_data_baseline.at[0, "chrblood_pl5vol"]
	if "chrblood_pl5vol" in sub_data_month2:
		pl_5_m2 = sub_data_month2.at[0, "chrblood_pl5vol"]
	if "chrblood_pl6vol" in sub_data_baseline:
		pl_6 = sub_data_baseline.at[0, "chrblood_pl6vol"]
	if "chrblood_pl6vol" in sub_data_month2:
		pl_6_m2 = sub_data_month2.at[0, "chrblood_pl6vol"]

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
	sub_data_baseline.at[0, 'number_blood_vials'] = np.count_nonzero(vials)	
	sub_data_month2.at[0, 'number_blood_vials'] = np.count_nonzero(vials_m2)

	print("Counting saliva vials")

	s_1 = s_2 = s_3 = s_4 = s_5 = s_6 = 0
	s_1_m2 = s_2_m2 = s_3_m2 = s_4_m2 = s_5_m2 = s_6_m2 = 0

	if "chrsaliva_vol1a" in sub_data_baseline:
		s_1 = sub_data_baseline.at[0, "chrsaliva_vol1a"]
	if "chrsaliva_vol1a" in sub_data_month2:
		s_1_m2 = sub_data_month2.at[0, "chrsaliva_vol1a"]
	if "chrsaliva_vol1b" in sub_data_baseline:
		s_2 = sub_data_baseline.at[0, "chrsaliva_vol1b"]
	if "chrsaliva_vol1b" in sub_data_month2:
		s_2_m2 = sub_data_month2.at[0, "chrsaliva_vol1b"]
	if "chrsaliva_vol2a" in sub_data_baseline:
		s_3 = sub_data_baseline.at[0, "chrsaliva_vol2a"]
	if "chrsaliva_vol2a" in sub_data_month2:
		s_3_m2 = sub_data_month2.at[0, "chrsaliva_vol2a"]
	if "chrsaliva_vol2b" in sub_data_baseline:
		s_4 = sub_data_baseline.at[0, "chrsaliva_vol2b"]
	if "chrsaliva_vol2b" in sub_data_month2:
		s_4_m2 = sub_data_month2.at[0, "chrsaliva_vol2b"]
	if "chrsaliva_vol3a" in sub_data_baseline:
		s_5 = sub_data_baseline.at[0, "chrsaliva_vol3a"]
	if "chrsaliva_vol3a" in sub_data_month2:
		s_5_m2 = sub_data_month2.at[0, "chrsaliva_vol3a"]
	if "chrsaliva_vol3b" in sub_data_baseline:
		s_6 = sub_data_baseline.at[0, "chrsaliva_vol3b"]
	if "chrsaliva_vol3b" in sub_data_month2:
		s_6_m2 = sub_data_month2.at[0, "chrsaliva_vol3b"]
				
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
	sub_data_baseline.at[0, 'number_saliva_vials'] = np.count_nonzero(vials)	
	sub_data_month2.at[0, 'number_saliva_vials'] = np.count_nonzero(vials_m2)




##### Calculating missingness for clinical forms
############ Missing variables for clinical data
	#perceived discimination scale
	if "chrdim_dim_yesno_q1_1" not in sub_data_baseline or "chrdlm_dim_yesno_q1_2" not in sub_data_baseline or "chrdlm_dim_sex" not in sub_data_baseline or "chrdlm_dim_yesno_age" not in sub_data_baseline or "chrdlm_dim_yesno_q4_1" not in sub_data_baseline or "chrdlm_dim_yesno_q5" not in sub_data_baseline or "chrdlm_dim_yesno_q3" not in sub_data_baseline or "chrdlm_dim_yesno_q6" not in sub_data_baseline or "chrdlm_dim_yesno_other" not in sub_data_baseline:
    		#print("Missing variables in json: ")
    		sub_data_baseline.at[0, 'no_missing_pdiscrims'] = 0

	else: # still could be missing data - check if any are null
		if pd.isnull(sub_data_baseline.at[0, "chrdim_dim_yesno_q1_1"]) or pd.isnull(sub_data_baseline.at[0, "chrdlm_dim_yesno_q1_2"]) or pd.isnull(sub_data_baseline.at[0, "chrdlm_dim_sex"]) or pd.isnull(sub_data_baseline.at[0, "chrdlm_dim_yesno_age"]) or pd.isnull(sub_data_baseline.at[0, "chrdlm_dim_yesno_q4_1"]) or pd.isnull(sub_data_baseline.at[0, "chrdlm_dim_yesno_q5"]) or pd.isnull(sub_data_baseline.at[0, "chrdlm_dim_yesno_q3"]) or pd.isnull(sub_data_baseline.at[0, "chrdlm_dim_yesno_q6"]) or pd.isnull(sub_data_baseline.at[0, "chrdlm_dim_yesno_other"]):
			sub_data_baseline.at[0, 'no_missing_pdiscrims'] = 0
		else:
			#print("None of the variables are missing")
			sub_data_baseline.at[0, 'no_missing_pdiscrims'] = 1

	# pubertal development scale
	pub_first_q = 0
	pub_m_q = 0
	pub_f_q = 0
	#print("First check if variables exist")
	if "chrpds_pds_1_p" in sub_data_baseline and "chrpds_pds_1_p" in sub_data_baseline and "chrpds_pds_2_p" in sub_data_baseline and "chrpds_pds_3_p" in sub_data_baseline:
		#print("No missing variables")
		if pd.isnull(sub_data_baseline.at[0, "chrpds_pds_1_p"]) or pd.isnull(sub_data_baseline.at[0, "chrpds_pds_2_p"]) or pd.isnull(sub_data_baseline.at[0, "chrpds_pds_3_p"]):
			#print("One is null")
			pub_first_q = 0 # missing data
		else:
			#print("No missing variables")
			pub_first_q = 1 # has all data

		# male questions
	if "chrdemo_sexassigned" in sub_data_baseline and sub_data_baseline.at[0, "chrdemo_sexassigned"] == "1" and "chrpds_pds_1_p" in sub_data_baseline and "chrpds_pds_m5_p" in sub_data_baseline:
		if pd.isnull(sub_data_baseline.at[0, "chrpds_pds_m4_p"]) or pd.isnull(sub_data_baseline.at[0, "chrpds_pds_m5_p"]):
			#print("Missing male variables")
			pub_m_q = 0 # missiing data
		else:
			#print("No missing male variables")
			pub_m_q = 1 # looks like has data
		# female questions
	if "chrdemo_sexassigned" in sub_data_baseline and sub_data_baseline.at[0, "chrdemo_sexassigned"] == "2" and "chrpds_pds_f4_p" in sub_data_baseline and "chrpds_pds_f5b_p" in sub_data_baseline:
		if pd.isnull(sub_data_baseline.at[0, "chrpds_pds_f4_p"]) or pd.isnull(sub_data_baseline.at[0, "chrpds_pds_f5b_p"]):
			#print("Missing female variables")
			pub_f_q = 0 #missing data
		else:
			pub_f_q = 1 #not missing data
		# all questions
	if pub_first_q == 1 and pub_m_q == 1 or pub_f_q == 1:
		#print("Not missing variables")
		sub_data_baseline.at[0, 'no_missing_pubds'] = "1"
	else:
		sub_data_baseline.at[0, 'no_missing_pubds'] = "0"

	#if name in ['nsipr']:
  # baseline
	if "chrnsipr_item1_rating" not in sub_data_baseline or "chrnsipr_item2_rating" not in sub_data_baseline or "chrnsipr_item3_rating" not in sub_data_baseline or "chrnsipr_item4_rating" not in sub_data_baseline or "chrnsipr_item5_rating" not in sub_data_baseline or "chrnsipr_item6_rating" not in sub_data_baseline or "chrnsipr_item7_rating" not in sub_data_baseline or "chrnsipr_item8_rating" not in sub_data_baseline or "chrnsipr_item9_rating" not in sub_data_baseline or "chrnsipr_item10_rating" not in sub_data_baseline or "chrnsipr_item11_rating" not in sub_data_baseline:
		#print("Missing variables")
		sub_data_baseline.at[0, 'no_missing_nsipr'] = "0" # missing data
		sub_data_month1.at[0, 'no_missing_nsipr'] = "0" 
		sub_data_month2.at[0, 'no_missing_nsipr'] = "0" 
		sub_data_month3.at[0, 'no_missing_nsipr'] = "0" 
		sub_data_month6.at[0, 'no_missing_nsipr'] = "0" 
	else:
		if pd.notna(sub_data_baseline.at[0, "chrnsipr_item1_rating"]) and pd.notna(sub_data_baseline.at[0, "chrnsipr_item2_rating"]) and pd.notna(sub_data_baseline.at[0, "chrnsipr_item3_rating"]) and pd.notna(sub_data_baseline.at[0, "chrnsipr_item4_rating"]) and pd.notna(sub_data_baseline.at[0, "chrnsipr_item5_rating"]) and pd.notna(sub_data_baseline.at[0, "chrnsipr_item6_rating"]) and pd.notna(sub_data_baseline.at[0, "chrnsipr_item7_rating"]) and pd.notna(sub_data_baseline.at[0, "chrnsipr_item8_rating"]) and pd.notna(sub_data_baseline.at[0, "chrnsipr_item9_rating"]) and pd.notna(sub_data_baseline.at[0, "chrnsipr_item10_rating"]) and pd.notna(sub_data_baseline.at[0, "chrnsipr_item11_rating"]):
			#print("yay no missing")
			sub_data_baseline.at[0, 'no_missing_nsipr'] = "1" # none are NA
		else:
			sub_data_baseline.at[0, 'no_missing_nsipr'] = "0"

		for vi in ["month1", "month2", "month3", "month6"]:
			var_list = ["chrnsipr_item1_rating", "chrnsipr_item2_rating", "chrnsipr_item3_rating", "chrnsipr_item4_rating", "chrnsipr_item5_rating", "chrnsipr_item6_rating", "chrnsipr_item7_rating", "chrnsipr_item8_rating", "chrnsipr_item9_rating", "chrnsipr_item10_rating"]
			print("NSI-PR month " + vi)
			visit_data = vars()['sub_data_' + str(vi)]

			if set(var_list).issubset(visit_data.columns) and visit_data[var_list].notna().values.any():
				print(" data")
				visit_data.loc[0, 'no_missing_nsipr'] = "1"
			else:
				print("No data")
				visit_data.loc[0, 'no_missing_nsipr'] = "0"
	
	#if name in ['cdss']:
  		# baseline
	if "chrcdss_calg1" not in sub_data_baseline or  "chrcdss_calg2" not in sub_data_baseline or "chrcdss_calg3" not in sub_data_baseline or "chrcdss_calg4" not in sub_data_baseline or "chrcdss_calg5" not in sub_data_baseline or "chrcdss_calg6" not in sub_data_baseline or "chrcdss_calg7" not in sub_data_baseline or "chrcdss_calg8" not in sub_data_baseline or "chrcdss_calg9" not in sub_data_baseline:
		#print("Missing variables")
		sub_data_baseline.at[0, 'no_missing_cdss'] = "0" #missing data
		sub_data_month1.at[0, 'no_missing_cdss'] = "0"
		sub_data_month2.at[0, 'no_missing_cdss'] = "0"
		sub_data_month3.at[0, 'no_missing_cdss'] = "0"
		sub_data_month6.at[0, 'no_missing_cdss'] = "0"
	else:
		if pd.notna(sub_data_baseline.at[0, "chrcdss_calg1"]) and pd.notna(sub_data_baseline.at[0, "chrcdss_calg2"]) and pd.notna(sub_data_baseline.at[0, "chrcdss_calg3"]) and pd.notna(sub_data_baseline.at[0, "chrcdss_calg4"]) and pd.notna(sub_data_baseline.at[0, "chrcdss_calg5"]) and pd.notna(sub_data_baseline.at[0, "chrcdss_calg6"]) and pd.notna(sub_data_baseline.at[0, "chrcdss_calg7"]) and pd.notna(sub_data_baseline.at[0, "chrcdss_calg8"]) and pd.notna(sub_data_baseline.at[0, "chrcdss_calg9"]):

			#print("No missin data yay")
			sub_data_baseline.at[0, 'no_missing_cdss'] = "1" #none are nan so not missing
		else:
			#print("Missing variables")
			sub_data_baseline.at[0, 'no_missing_cdss'] = "0"

		for vi in ["month1", "month2", "month3", "month6"]:
			var_list = ["chrcdss_calg1", "chrcdss_calg2", "chrcdss_calg3", "chrcdss_calg4", "chrcdss_calg5", "chrcdss_calg6", "chrcdss_calg7", "chrcdss_calg8", "chrcdss_calg9"]
			print("CDSS " + vi)
			visit_data = vars()['sub_data_' + str(vi)]

			if set(var_list).issubset(visit_data.columns) and visit_data[var_list].notna().values.any():
				print("Data available")
				visit_data.loc[0, 'no_missing_cdss'] = "1"
			else:
				print("No data")
				visit_data.loc[0, 'no_missing_cdss'] = "0"

	#if name in ['oasis']:
	# baseline
	if "chroasis_oasis_1" not in sub_data_baseline or  "chroasis_oasis_2" not in sub_data_baseline or "chroasis_oasis_3" not in sub_data_baseline or "chroasis_oasis_4" not in sub_data_baseline or "chroasis_oasis_2" not in sub_data_baseline:
		#print("Missing variables nay")
		sub_data_baseline.at[0, 'no_missing_oasis'] = "0" # something missing
		sub_data_month1.at[0, 'no_missing_oasis'] = "0"
		sub_data_month2.at[0, 'no_missing_oasis'] = "0"
		sub_data_month3.at[0, 'no_missing_oasis'] = "0"
		sub_data_month6.at[0, 'no_missing_oasis'] = "0"
	else:
		if pd.notna(sub_data_baseline.at[0, "chroasis_oasis_1"]) and pd.notna(sub_data_baseline.at[0, "chroasis_oasis_2"]) and pd.notna(sub_data_baseline.at[0, "chroasis_oasis_3"]) and pd.notna(sub_data_baseline.at[0, "chroasis_oasis_4"]) and pd.notna(sub_data_baseline.at[0, "chroasis_oasis_5"]):
			#print("yay")
			sub_data_baseline.at[0, 'no_missing_oasis'] = "1" # nothing missing or na
		else:
			sub_data_baseline.at[0, 'no_missing_oasis'] = "0"


		for vi in ["month1", "month2", "month3", "month6"]:
			var_list = ["chroasis_oasis_1", "chroasis_oasis_2", "chroasis_oasis_3", "chroasis_oasis_4", "chroasis_oasis_5"]
			print("OASIS " + vi)
			visit_data = vars()['sub_data_' + str(vi)]

			if set(var_list).issubset(visit_data.columns) and visit_data[var_list].notna().values.any():
				print("Data available")
				visit_data.loc[0, 'no_missing_oasis'] = "1"
			else:
				print("No data")
				visit_data.loc[0, 'no_missing_oasis'] = "0"

	#if name in ['pss']:
		# baseline
	if "chrpss_pssp1_1" not in sub_data_baseline or  "chrpss_pssp1_1" not in sub_data_baseline or "chrpss_pssp1_2" not in sub_data_baseline or "chrpss_pssp1_3" not in sub_data_baseline or "chrpss_pssp2_1" not in sub_data_baseline or "chrpss_pssp2_3" not in sub_data_baseline or "chrpss_pssp2_4" not in sub_data_baseline or "chrpss_pssp2_5" not in sub_data_baseline or "chrpss_pssp3_1" not in sub_data_baseline or "chrpss_pssp3_4" not in sub_data_baseline:
		#print("Missing variables")
		sub_data_baseline.at[0, 'no_missing_pss'] = "0" #missing data
		sub_data_month1.at[0, 'no_missing_pss'] = "0"
		sub_data_month2.at[0, 'no_missing_pss'] = "0"
		sub_data_month3.at[0, 'no_missing_pss'] = "0"
		sub_data_month6.at[0, 'no_missing_pss'] = "0"
	else:
		if pd.isnull(sub_data_baseline.at[0, "chrpss_pssp1_1"]) or pd.isnull(sub_data_baseline.at[0, "chrpss_pssp1_2"]) or pd.isnull(sub_data_baseline.at[0, "chrpss_pssp1_3"]) or pd.isnull(sub_data_baseline.at[0, "chrpss_pssp2_1"]) or pd.isnull(sub_data_baseline.at[0, "chrpss_pssp2_2"]) or pd.isnull(sub_data_baseline.at[0, "chrpss_pssp2_3"]) or pd.isnull(sub_data_baseline.at[0, "chrpss_pssp2_4"]) or pd.isnull(sub_data_baseline.at[0, "chrpss_pssp2_5"]) or pd.isnull(sub_data_baseline.at[0, "chrpss_pssp3_1"]) or pd.isnull(sub_data_baseline.at[0, "chrpss_pssp3_4"]):
			sub_data_baseline.at[0, 'no_missing_pss'] = "0"
		else:
			sub_data_baseline.at[0, 'no_missing_pss'] = "1" #none are null

		for vi in ["month1", "month2", "month3", "month6"]:
			var_list = ["chrpss_pssp1_1", "chrpss_pssp1_2", "chrpss_pssp1_3", "chrpss_pssp2_1", "chrpss_pssp2_2", "chrpss_pssp2_3", "chrpss_pssp2_4", "chrpss_pssp2_5", "chrpss_pssp3_1", "chrpss_pssp3_4"]
			print("pss " + vi)
			visit_data = vars()['sub_data_' + str(vi)]

			if set(var_list).issubset(visit_data.columns) and visit_data[var_list].notna().values.any():
				print("Data available")
				visit_data.loc[0, 'no_missing_pss'] = "1"
			else:
				print("No data")
				visit_data.loc[0, 'no_missing_pss'] = "0"

	#if name in ['item_promis_for_sleep']:
	# baseline
	if "chrpromis_sleep109" not in sub_data_baseline or "chrpromis_sleep109" not in sub_data_baseline or "chrpromis_sleep116" not in sub_data_baseline or "chrpromis_sleep20" not in sub_data_baseline or "chrpromis_sleep44" not in sub_data_baseline or "chrpromise_sleep108" not in sub_data_baseline or "chrpromis_sleep72" not in sub_data_baseline or "chrpromis_sleep67" not in sub_data_baseline or "chrpromis_sleep115" not in sub_data_baseline:
		#print("Missing variables")
		sub_data_baseline.at[0, 'no_missing_promis'] = "0"
		sub_data_month1.at[0, 'no_missing_promis'] = "0"
		sub_data_month2.at[0, 'no_missing_promis'] = "0"
		sub_data_month3.at[0, 'no_missing_promis'] = "0"
		sub_data_month6.at[0, 'no_missing_promis'] = "0"
	else:
		if pd.isnull(sub_data_baseline.at[0, "chrpromis_sleep109"]) or pd.isnull(sub_data_baseline.at[0, "chrpromis_sleep116"]) or pd.isnull(sub_data_baseline.at[0, "chrpromis_sleep20"]) or pd.isnull(sub_data_baseline.at[0, "chrpromis_sleep44"]) or pd.isnull(sub_data_baseline.at[0, "chrpromise_sleep108"]) or pd.isnull(sub_data_baseline.at[0, "chrpromis_sleep72"]) or pd.isnull(sub_data_baseline.at[0, "chrpromis_sleep67"]) or pd.isnull(sub_data_baseline.at[0, "chrpromis_sleep115"]):

			#print("Missing data")
			sub_data_baseline.at[0, 'no_missing_promis'] = "0"

		else:
			#print("No missing data")
			sub_data_baseline.at[0, 'no_missing_promis'] = "1"

		for vi in ["month1", "month2", "month3", "month6"]:
			var_list = ["chrpromis_sleep109", "chrpromis_sleep116", "chrpromis_sleep20", "chrpromis_sleep44", "chrpromise_sleep108", "chrpromis_sleep72", "chrpromis_sleep67", "chrpromis_sleep115"]
			print("promis " + vi)
			visit_data = vars()['sub_data_' + str(vi)]

			if set(var_list).issubset(visit_data.columns) and visit_data[var_list].notna().values.any():
				print("Data available")
				visit_data.loc[0, 'no_missing_promis'] = "1"
			else:
				print("No data")
				visit_data.loc[0, 'no_missing_promis'] = "0"

	#if name in ['bprs']:
	# baseline
	if "chrbprs_bprs_somc" in sub_data_baseline:
		if "chrbprs_bprs_total" not in sub_data_baseline:
			sub_data_baseline.at[0, 'no_missing_bprs'] = "0"
			sub_data_month1.at[0, 'no_missing_bprs'] = "0"
			sub_data_month2.at[0, 'no_missing_bprs'] = "0"
			sub_data_month3.at[0, 'no_missing_bprs'] = "0"
			sub_data_month4.at[0, 'no_missing_bprs'] = "0"
			sub_data_month5.at[0, 'no_missing_bprs'] = "0"
			sub_data_month6.at[0, 'no_missing_bprs'] = "0"
		else:
			if pd.notna(sub_data_baseline.at[0, "chrbprs_bprs_total"]) and sub_data_baseline.at[0, "chrbprs_bprs_total"] != 999:
				sub_data_baseline.at[0, 'no_missing_bprs'] = "1"
			else:
				sub_data_baseline.at[0, 'no_missing_bprs'] = "0"

		for vi in ["month1", "month2", "month3", "month4", "month5", "month6"]:
			var_list = ["chrbprs_bprs_total"]
			print("bprs " + vi)
			visit_data = vars()['sub_data_' + str(vi)]

			if set(var_list).issubset(visit_data.columns) and visit_data[var_list].notna().values.any():
				print("Data available")
				visit_data.loc[0, 'no_missing_bprs'] = "1"
			else:
				print("No data")
				visit_data.loc[0, 'no_missing_bprs'] = "0"

	#if name in ['cssrs_baseline']:
		# baseline
	if "chrcssrsb_si1l" not in sub_data_baseline or "chrcssrsb_si2l" not in sub_data_baseline or "chrcssrsb_si3l" not in sub_data_baseline or "chrcssrsb_si4l" not in sub_data_baseline or "chrcssrsb_si5l" not in sub_data_baseline or "chrcssrsb_css_sim1" not in sub_data_baseline or "chrcssrsb_css_sim2" not in sub_data_baseline or "chrcssrsb_css_sim3" not in sub_data_baseline or "chrcssrsb_css_sim4" not in sub_data_baseline or "chrcssrsb_css_sim5" not in sub_data_baseline:
		sub_data_baseline.at[0, 'no_missing_cdssrsb'] = "0"
	else:
		if pd.notna(sub_data_baseline.at[0, "chrcssrsb_si1l"]) and pd.notna(sub_data_baseline.at[0, "chrcssrsb_si2l"]) and pd.notna(sub_data_baseline.at[0, "chrcssrsb_si3l"]) and pd.notna(sub_data_baseline.at[0, "chrcssrsb_si4l"]) and pd.notna(sub_data_baseline.at[0, "chrcssrsb_si5l"]) and pd.notna(sub_data_baseline.at[0, "chrcssrsb_css_sim1"]) and pd.notna(sub_data_baseline.at[0, "chrcssrsb_css_sim2"]) and pd.notna(sub_data_baseline.at[0, "chrcssrsb_css_sim3"]) and pd.notna(sub_data_baseline.at[0, "chrcssrsb_css_sim4"]) and pd.notna(sub_data_baseline.at[0, "chrcssrsb_css_sim5"]):
			#print("No Missing values")
			sub_data_baseline.at[0, 'no_missing_cdssrsb'] = "1" # none is na
		else:
			sub_data_baseline.at[0, 'no_missing_cdssrsb'] = "0"

	#if name in ['cssrs_followup']:
	# baseline
	if "chrcssrsfu_si1l" not in sub_data_month1 or  "chrcssrsfu_si2l" not in sub_data_month1 or "chrcssrsfu_si3l" not in sub_data_month1 or "chrcssrsfu_si4l" not in sub_data_month1 or "chrcssrsfu_si5l" not in sub_data_month1 or "chrcssrsfu_css_sim1" not in sub_data_month1 or "chrcssrsfu_css_sim2" not in sub_data_month1 or "chrcssrsfu_css_sim3" not in sub_data_month1 or "chrcssrsfu_css_sim4" not in sub_data_month1 or "chrcssrsfu_css_sim5" not in sub_data_month1:
		#print("Missing variables")
		sub_data_month1.at[0, 'no_missing_cdssrsfu'] = "0"
	else:
		if pd.notna(sub_data_month1.at[0, "chrcssrsfu_si1l"]) and pd.notna(sub_data_month1.at[0, "chrcssrsfu_si2l"]) and pd.notna(sub_data_month1.at[0, "chrcssrsfu_si3l"]) and pd.notna(sub_data_month1.at[0, "chrcssrsfu_si4l"]) and pd.notna(sub_data_month1.at[0, "chrcssrsfu_si5l"]) and pd.notna(sub_data_month1.at[0, "chrcssrsfu_css_sim1"]) and pd.notna(sub_data_month1.at[0, "chrcssrsfu_css_sim2"]) and pd.notna(sub_data_month1.at[0, "chrcssrsfu_css_sim3"]) and pd.notna(sub_data_month1.at[0, "chrcssrsfu_css_sim4"]) and pd.notna(sub_data_month1.at[0, "chrcssrsfu_css_sim5"]):
			#print("No missing values")
			sub_data_month1.at[0, 'no_missing_cdssrsfu'] = "1"
		else:
			sub_data_month1.at[0, 'no_missing_cdssrsfu'] = "0"

	#if name in ['premorbid_adjustment_scale']:
	# baseline
	pas_6_11_q = 0
	pas_12_15_q = 0
	pas_16_18_q = 0
	pub_19_q = 0
	if "chrpas_pmod_child1" not in sub_data_month1 or  "chrpas_pmod_child1" not in sub_data_month1 or "chrpas_pmod_child2" not in sub_data_month1 or "chrpas_pmod_child3" not in sub_data_month1 or "chrpas_pmod_child4" not in sub_data_month1:
		#print("Missing variables")
		pas_6_11_q = 0
	else:
		if pd.isnull(sub_data_month1.at[0, "chrpas_pmod_child1"]) or pd.isnull(sub_data_month1.at[0, "chrpas_pmod_child2"]) or pd.isnull(sub_data_month1.at[0, "chrpas_pmod_child3"]) or pd.isnull(sub_data_month1.at[0, "chrpas_pmod_child4"]):
			pas_6_11_q = 0
		else:
			#print("Complete variables")
			pas_6_11_q = 1
	# early adol
	if "chrpas_pmod_adol_early1" not in sub_data_month1 or "chrpas_pmod_adol_early2" not in sub_data_month1 or "chrpas_pmod_adol_early3" not in sub_data_month1 or "chrpas_pmod_adol_early4" not in sub_data_month1 or "chrpas_pmod_adol_early5" not in sub_data_month1:
		pub_12_15_q = 0
	else:
		if pd.notna(sub_data_month1.at[0, "chrpas_pmod_adol_early1"]) or pd.notna(sub_data_month1.at[0, "chrpas_pmod_adol_early2"]) or pd.notna(sub_data_month1.at[0, "chrpas_pmod_adol_early3"]) or pd.notna(sub_data_month1.at[0, "chrpas_pmod_adol_early4"]) or pd.notna(sub_data_month1.at[0, "chrpas_pmod_adol_early5"]) or pd.isnull(sub_data_month1.at[0, "chrpas_pmod_child4"]):
			#print("Not missing early adol data")
			pub_12_15_q = 1

	#late adol
	if "chrpas_pmod_adol_late1" not in sub_data_month1 or "chrpas_pmod_adol_late2" not in sub_data_month1 or "chrpas_pmod_adol_late3" not in sub_data_month1 or "chrpas_pmod_adol_late4" not in sub_data_month1 or "chrpas_pmod_adol_late5" not in sub_data_month1:
		pub_16_18_q = 0
	else:
		if pd.notna(sub_data_month1.at[0, "chrpas_pmod_adol_late1"]) or pd.notna(sub_data_month1.at[0, "chrpas_pmod_adol_late2"]) or pd.notna(sub_data_month1.at[0, "chrpas_pmod_adol_late3"]) or pd.notna(sub_data_month1.at[0, "chrpas_pmod_adol_late4"]) or pd.notna(sub_data_month1.at[0, "chrpas_pmod_adol_late5"]):
			#print("Not missing late adol")
			pub_16_18_q = 1

	#adult
	if "chrpas_pmod_adult1" not in sub_data_month1 or "chrpas_pmod_adult2" not in sub_data_month1 or ("chrpas_pmod_adult3v1" not in sub_data_month1 and "chrpas_pmod_adult3v3" not in sub_data_month1):
		pub_19_q = 0
	else:
		if pd.isnull(sub_data_month1.at[0, "chrpas_pmod_adult1"]) or pd.isnull(sub_data_month1.at[0, "chrpas_pmod_adult2"]):
			#print("Not missing adult data")
			pub_19_q = 1
		else:
			pub_19_q = 0
	# all questions
	sub_data_month1.at[0, 'no_missing_pas'] = "0"
	if age < 15 and pas_6_11_q == 1:
		sub_data_month1.at[0, 'no_missing_pas'] = "1"
	if age < 18 and pas_6_11_q == 1 and pub_12_15_q == 1:
		sub_data_month1.at[0, 'no_missing_pas'] = "1"
	if age < 19 and pas_6_11_q == 1 and pub_12_15_q == 1 and pub_16_18_q == 1:
		sub_data_month1.at[0, 'no_missing_pas'] = "1"
	if age > 19 and pas_6_11_q == 1 and pub_12_15_q == 1 and pub_16_18_q == 1 and pub_19_q == 1:
		sub_data_month1.at[0, 'no_missing_pas'] = "1"

	## screening sofas
	if "chrsofas_premorbid" not in sub_data or  "chrsofas_currscore12mo" not in sub_data or "chrsofas_currscore" not in sub_data or "chrsofas_lowscore" not in sub_data:
		#print("Missing variables nay")
		sub_data.at[0, 'no_missing_sofas'] = "0" # something missing
	else:
		if pd.notna(sub_data.at[0, "chrsofas_premorbid"]) and pd.notna(sub_data.at[0, "chrsofas_currscore12mo"]) and pd.notna(sub_data.at[0, "chrsofas_currscore"]) and pd.notna(sub_data.at[0, "chrsofas_lowscore"]):
			#print("yay")
			sub_data.at[0, 'no_missing_sofas'] = "1" # nothing missing or na
		else:
			sub_data.at[0, 'no_missing_sofas'] = "0"
	
	#baseline sofas
	if "chrsofas_premorbid" not in sub_data_baseline or  "chrsofas_currscore12mo" not in sub_data_baseline or "chrsofas_currscore" not in sub_data_baseline or "chrsofas_lowscore" not in sub_data_baseline:
		#print("Missing variables nay")
		sub_data_baseline.at[0, 'no_missing_sofas'] = "0" # something missing
	else:
		if pd.notna(sub_data_baseline.at[0, "chrsofas_premorbid"]) and pd.notna(sub_data_baseline.at[0, "chrsofas_currscore12mo"]) and pd.notna(sub_data_baseline.at[0, "chrsofas_currscore"]) and pd.notna(sub_data_baseline.at[0, "chrsofas_lowscore"]):
			#print("yay")
			sub_data_baseline.at[0, 'no_missing_sofas'] = "1" # nothing missing or na
		else:
			sub_data_baseline.at[0, 'no_missing_sofas'] = "0"

	# informed consent run
	if "chric_consent_date" not in sub_data or  "chric_passive" not in sub_data or "chric_surveys" not in sub_data or "chric_actigraphy" not in sub_data:
		#print("Missing variables nay")
		sub_data.at[0, 'no_missing_consent'] = "0" # something missing
	else:
		if pd.notna(sub_data.at[0, "chric_consent_date"]) and pd.notna(sub_data.at[0, "chric_passive"]) and pd.notna(sub_data.at[0, "chric_surveys"]) and pd.notna(sub_data.at[0, "chric_actigraphy"]):
			#print("yay")
			sub_data.at[0, 'no_missing_consent'] = "1" # nothing missing or na
		else:
			sub_data.at[0, 'no_missing_consent'] = "0"

	#baseline global functioning role scale
	if "chrgfr_prompt2" not in sub_data_baseline or  "chrgfr_prompt3" not in sub_data_baseline or "chrgfr_prompt4" not in sub_data_baseline or "chrgfr_gf_primaryrole" not in sub_data_baseline or "chrgfr_gf_role_scole" not in sub_data_baseline or "chrgfr_gf_role_low" not in sub_data_baseline or "chrgfr_gf_role_high" not in sub_data_baseline:
		#print("Missing variables nay")
		sub_data_baseline.at[0, 'no_missing_glob_func_role'] = "0" # something missing
	else:
		if pd.notna(sub_data_baseline.at[0, "chrgfr_prompt2"]) and pd.notna(sub_data_baseline.at[0, "chrgfr_prompt3"]) and pd.notna(sub_data_baseline.at[0, "chrgfr_prompt4"]) and pd.notna(sub_data_baseline.at[0, "chrgfr_gf_primaryrole"]) and pd.notna(sub_data_baseline.at[0, "chrgfr_gf_role_scole"]) and pd.notna(sub_data_baseline.at[0, "chrgfr_gf_role_low"]) and pd.notna(sub_data_baseline.at[0, "chrgfr_gf_role_high"]):
			#print("yay")
			sub_data_baseline.at[0, 'no_missing_glob_func_role'] = "1" # nothing missing or na
		else:
			sub_data_baseline.at[0, 'no_missing_glob_func_role'] = "0"

	# baseline global functioning social
	if "chrgfs_overallsat" not in sub_data_baseline or  "chrgfs_gf_social_scale" not in sub_data_baseline or "chrgfs_gf_social_low" not in sub_data_baseline or "chrgfs_gf_social_high" not in sub_data_baseline:
		#print("Missing variables nay")
		sub_data_baseline.at[0, 'no_missing_glob_func_social'] = "0" # something missing
	else:
		if pd.notna(sub_data_baseline.at[0, "chrgfs_overallsat"]) and pd.notna(sub_data_baseline.at[0, "chrgfs_gf_social_scale"]) and pd.notna(sub_data_baseline.at[0, "chrgfs_gf_social_low"]) and pd.notna(sub_data_baseline.at[0, "chrgfs_gf_social_high"]):
			#print("yay")
			sub_data_baseline.at[0, 'no_missing_glob_func_social'] = "1" # nothing missing or na
		else:
			sub_data_baseline.at[0, 'no_missing_glob_func_social'] = "0"

	## Penn CNB baseline
	if "chrpenn_complete" not in sub_data_baseline or  "chrpenn_missing_1" not in sub_data_baseline:
		#print("Missing variables nay")
		sub_data_baseline.at[0, 'no_missing_penncnb'] = "0" # something missing
	else:
		if pd.notna(sub_data_baseline.at[0, "chrpenn_complete"]) and pd.notna(sub_data_baseline.at[0, "chrpenn_missing_1"]):
			#print("yay")
			sub_data_baseline.at[0, 'no_missing_penncnb'] = "1" # nothing missing or na
		else:
			sub_data_baseline.at[0, 'no_missing_penncnb'] = "0"

	## WRAT baseline
	if "chrpreiq_reading_task" not in sub_data_baseline or  "chrpreiq_total_raw" not in sub_data_baseline or "chrpreiq_standard_score" not in sub_data_baseline:
		#print("Missing variables nay")
		sub_data_baseline.at[0, 'no_missing_wrat'] = "0" # something missing
	else:
		if pd.notna(sub_data_baseline.at[0, "chrpreiq_reading_task"]) and pd.notna(sub_data_baseline.at[0, "chrpreiq_total_raw"]) and pd.notna(sub_data_baseline.at[0, "chrpreiq_standard_score"]):
			#print("yay")
			sub_data_baseline.at[0, 'no_missing_wrat'] = "1" # nothing missing or na
		else:
			sub_data_baseline.at[0, 'no_missing_wrat'] = "0"

	# WASII IQ baseline - need to add in variables that depend on which WASI used!
	if "chriq_assessment" not in sub_data_baseline or "chriq_vocab_raw" not in sub_data_baseline or "chriq_matrix_raw" not in sub_data_baseline or "chriq_fsiq" not in sub_data_baseline:
		#print("Missing variables nay")
		sub_data_baseline.at[0, 'no_missing_wasiiq'] = "0" # something missing
	else:
		if pd.notna(sub_data_baseline.at[0, "chriq_assessment"]) and pd.notna(sub_data_baseline.at[0, "chriq_vocab_raw"]) and pd.notna(sub_data_baseline.at[0, "chriq_matrix_raw"]) and pd.notna(sub_data_baseline.at[0, "chriq_fsiq"]):
			#print("yay")
			sub_data_baseline.at[0, 'no_missing_wasiiq'] = "1" # nothing missing or na
		else:
			sub_data_baseline.at[0, 'no_missing_wasiiq'] = "0"


	print("check")
	#getting percentages for each of the forms
	percentage_file = "/data/predict1/data_from_nda/formqc/{0}-{1}-percentage.csv".format(site, id)
	status = "0"
	screening_perc = pd.DataFrame()
	baseline_perc = pd.DataFrame()
	month1_perc = pd.DataFrame()
	month2_perc = pd.DataFrame()
	month3_perc = pd.DataFrame()
	month4_perc = pd.DataFrame()
	month5_perc = pd.DataFrame()
	month6_perc = pd.DataFrame()
	month7_perc = pd.DataFrame()
	month8_perc = pd.DataFrame()
	conversion_perc = pd.DataFrame()


	if os.path.exists(percentage_file):
		print("Reading in percentage file")
		percentages = pd.read_csv(percentage_file, sep= ",", index_col= 0)
		print(percentages)

		screening_perc = pd.DataFrame(percentages.loc[screening])
		baseline_perc = pd.DataFrame(percentages.loc[baseline])
		conversion_perc = pd.DataFrame(percentages.loc[conversion])

		screening_perc = screening_perc.transpose()
		baseline_perc = baseline_perc.transpose()
		conversion_perc = conversion_perc.transpose()

		screening_perc = screening_perc.reset_index(drop=True)
		baseline_perc = baseline_perc.reset_index(drop=True)
		conversion_perc = conversion_perc.reset_index(drop=True)
		print("Screening percentage")
		print(screening_perc)

		for vi in ["1", "2", "3", "4", "5", "6", "7", "8"]:
			#print(vi)
			#visit_name = ('month_' + str(vi) + '_')
			visit_perc = pd.DataFrame()

			evlist = [i for i in all_events if i.startswith('month_' + str(vi) + "_")]
			if evlist == []:
				name = []
			else:
				name = evlist[0]

			if name != [] and name in percentages.index:
				visit_perc = pd.DataFrame(percentages.loc[[name]])
				#visit_perc = visit_perc.transpose()
				#print('Month percentage')
				#print(visit_perc)
				globals()['month' + str(vi) + "_perc"] = visit_perc.reset_index(drop=True)

		#print(month1_perc)
		# getting the visit status
		perc_check = percentages
		perc_check = perc_check.drop('informed_consent_run_sheet' , axis='columns')
		perc_check = perc_check[perc_check.index.str.contains('floating')==False]
		#print("check")
		perc_check['Completed']= perc_check.iloc[:, 1:].sum(axis=1)
		perc_check['Total_empty'] = (perc_check == 0).sum(axis=1)
		#print(perc_check)
		perc_check_2 = perc_check[perc_check.Completed > 100]
		perc_check = perc_check[perc_check.index.str.contains('convers')==True]
		perc_check = perc_check.reset_index()
		print("Printing visit perc")
		print(perc_check_2)
		print("Printing conversion perc")
		print(perc_check)

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

	
	# adding removed status
	status_removed = "0"
	# screening
	if "chrmiss_withdrawn" in sub_data and sub_data.at[0, "chrmiss_withdrawn"] == '1':
		status_removed = "1"
	if "chrmiss_discon" in sub_data and sub_data.at[0, "chrmiss_discon"] == '1':
		status_removed = "1"

	# baseline
	if "chrmiss_withdrawn" in sub_data_baseline and sub_data_baseline.at[0, "chrmiss_withdrawn"] == '1':
		status_removed = "1"
	if "chrmiss_discon" in sub_data_baseline and sub_data_baseline.at[0, "chrmiss_discon"] == '1':
		status_removed = "1"

	#month1
	if "chrmiss_withdrawn" in sub_data_month1 and sub_data_month1.at[0, "chrmiss_withdrawn"] == '1':
		status_removed = "1"
	if "chrmiss_discon" in sub_data_month1 and sub_data_month1.at[0, "chrmiss_discon"] == '1':
		status_removed = "1"

	#month2
	if "chrmiss_withdrawn" in sub_data_month2 and sub_data_month2.at[0, "chrmiss_withdrawn"] == '1':
		status_removed = "1"
	if "chrmiss_discon" in sub_data_month2 and sub_data_month2.at[0, "chrmiss_discon"] == '1':
		status_removed = "1"

	#month3
	if "chrmiss_withdrawn" in sub_data_month3 and sub_data_month3.at[0, "chrmiss_withdrawn"] == '1':
		status_removed = "1"
	if "chrmiss_discon" in sub_data_month3 and sub_data_month3.at[0, "chrmiss_discon"] == '1':
		status_removed = "1"

	#month4
	if "chrmiss_withdrawn" in sub_data_month4 and sub_data_month4.at[0, "chrmiss_withdrawn"] == '1':
		status_removed = "1"
	if "chrmiss_discon" in sub_data_month4 and sub_data_month4.at[0, "chrmiss_discon"] == '1':
		status_removed = "1"
	
	print("Visit status: " + str(status))
	print("Removed status: " + str(status_removed))
		

	#print(screening_perc.T)
	#print(baseline_perc.T)

	# setting up dpdash columns for screening, baseline, and month 1
	names_dash = ['reftime','day', 'timeofday', 'weekday', 'days_since_consent', 'subjectid', 'site', 'mtime', 'visit_status']
	dpdash_main = pd.DataFrame(columns = names_dash)
	dpdash_main.at[0, 'subjectid'] = id
	dpdash_main.at[0, 'site'] = site
	dpdash_main.at[0, 'mtime'] = sub_data.at[0, "chric_consent_date"]
	dpdash_main.at[0, 'day'] = 1
	dpdash_main.at[0, 'days_since_consent'] = days_between(sub_data.at[0, "chric_consent_date"], today)
	dpdash_main.at[0, 'weeks_since_consent'] = round(dpdash_main.at[0, 'days_since_consent'] / 7)
	dpdash_main.at[0, 'visit_status'] = status
	dpdash_main.at[0, 'status_removed'] = status_removed
	dpdash_main.at[0, 'file_updated'] = last_date
	time_since_update = days_between(today, last_date)
	if time_since_update > 49 and status != "0" and status_removed != "1": # It's been over 7 weeks since we received data
		dpdash_main.at[0, 'check_last_updated'] = 1
	else:
		dpdash_main.at[0, 'check_last_updated'] = 0
	if time_since_update < 8: # Updated in last 7 days
		dpdash_main.at[0, 'recently_updated'] = 1
	else:
		dpdash_main.at[0, 'recently_updated'] = 0
	
	if status == "0":
		dpdash_main.at[0, 'visit_status_string'] = "consent"
	if status == 1:
		dpdash_main.at[0, 'visit_status_string'] = "screen"
	if status == 2:
		dpdash_main.at[0, 'visit_status_string'] = "baseln"
	if status == 3:
		dpdash_main.at[0, 'visit_status_string'] = "month1"
	if status == 4:
		dpdash_main.at[0, 'visit_status_string'] = "month2"
	if status == 5:
		dpdash_main.at[0, 'visit_status_string'] = "month3"
	if status == 6:
		dpdash_main.at[0, 'visit_status_string'] = "month4"
	if status == 7:
		dpdash_main.at[0, 'visit_status_string'] = "month5"
	if status == 8:
		dpdash_main.at[0, 'visit_status_string'] = "month6"
	if status == 9:
		dpdash_main.at[0, 'visit_status_string'] = "month7"
	if status == 10:
		dpdash_main.at[0, 'visit_status_string'] = "month8"
	if status == 11:
		dpdash_main.at[0, 'visit_status_string'] = "month9"
	if status == 12:
		dpdash_main.at[0, 'visit_status_string'] = "month10"
	if status == 13:
		dpdash_main.at[0, 'visit_status_string'] = "month11"
	if status == 14:
		dpdash_main.at[0, 'visit_status_string'] = "month12"
	if status == 15:
		dpdash_main.at[0, 'visit_status_string'] = "month18"
	if status == 16:
		dpdash_main.at[0, 'visit_status_string'] = "month24"
	if status == '98':
		dpdash_main.at[0, 'visit_status_string'] = "converted"
	if status_removed == "1":
		dpdash_main.at[0, 'visit_status_string'] = "removed"

	print("Visit Status: ")
	print(str(dpdash_main.at[0, 'visit_status_string']))

	dpdash_main = dpdash_main.reset_index(drop=True)

	# concatenating screening and dpdash main
	sub_data = sub_data.reset_index(drop=True)

	frames = [dpdash_main, sub_data, screening_perc]
	#print("Checking frames:")
	#print(screening_perc)
	dpdash_full = pd.concat(frames, axis=1)

	dpdash_full.dropna(how='all', axis=0, inplace=True)
	print("Screening csv for participant: ")
	print(dpdash_full.T)
	
	id_tracker["Screening_{0}".format(id)] = dpdash_full

	# concatenating dpdash main and baseline
	sub_data_baseline = sub_data_baseline.reset_index(drop=True)

	frames = [dpdash_main, sub_data_baseline, baseline_perc]
	dpdash_full = pd.concat(frames, axis=1)
	dpdash_full.dropna(how='all', axis=0, inplace=True)

	id_baseline_tracker["Baseline_{0}".format(id)] = dpdash_full

	# saving conversion visit
	sub_data_conversion = sub_data_conversion.reset_index(drop=True)

	frames = [dpdash_main, sub_data_conversion, conversion_perc]
	dpdash_full = pd.concat(frames, axis=1)
	dpdash_full.dropna(how='all', axis=0, inplace=True)

	id_conversion_tracker["Conversion_{0}".format(id)] = dpdash_full

	# concatenating dpdash main and month1, month2, month3, month4
	#sub_data_month1 = sub_data_month1.reset_index(drop=True)
	for vi in ["1", "2", "3", "4", "5", "6", "7", "8"]:
		print("Month " + vi)

		visit_tracker = vars()['id_month' + str(vi) + '_tracker']
		visit_data = vars()['sub_data_month' + str(vi)]
		visit_perc = vars()['month' + str(vi) + "_perc"]
	
		visit_data = visit_data.reset_index(drop=True)
		frames = [dpdash_main, visit_data, visit_perc]
		dpdash_full = pd.concat(frames, axis=1)

		#print("Month " + str(vi) + " csv for participant: ")
		dpdash_full.dropna(how='all', axis=0, inplace=True)
		#print(dpdash_full.T)

		visit_tracker["Month{0}_{1}".format(vi, id)] = dpdash_full


## Concatenating all participant data together
# screening visit
final_csv = pd.concat(id_tracker, ignore_index=True)
print("Screening CSV")
print(final_csv)

if "chrap_total" in final_csv:
	final_csv[["chrap_total"]] = final_csv[["chrap_total"]].apply(pd.to_numeric)
	final_csv = final_csv.round({"chrap_total":1})

print("Creating and saving screening and baseline csvs")
numbers = list(range(1,(len(final_csv.index) +1))) # changing day numbers to sequence
final_csv['day'] = numbers

final_csv = final_csv.sort_values(['days_since_consent', 'day'])
final_csv['day'] = numbers
numbers.sort(reverse = True)
final_csv['num'] = numbers
	
# baseline visit
final_baseline_csv = pd.concat(id_baseline_tracker, ignore_index=True)
numbers = list(range(1,(len(final_baseline_csv.index) +1))) # changing day numbers to sequence
final_baseline_csv['day'] = numbers

final_baseline_csv = final_baseline_csv.sort_values(['days_since_consent', 'day'])
final_baseline_csv['day'] = numbers
numbers.sort(reverse = True)
final_baseline_csv['num'] = numbers

###### Saving combined csvs
final_csv.to_csv(output1 + "combined-PRONET-form_screening-day1to1.csv", sep=',', index = False, header=True)
final_baseline_csv.to_csv(output1 + "combined-PRONET-form_baseline-day1to1.csv", sep=',', index = False, header=True)

for vi in ["month1", "month2", "month3", "month4", "month5", "month6", "month7", "month8"]:
	
	print(vi)
	tracker_name = vars()['id_' + str(vi) + '_tracker']
	concat_csv = pd.concat(tracker_name, ignore_index=True)

	numbers = list(range(1,(len(concat_csv.index) +1))) # changing day numbers to sequence
	concat_csv['day'] = numbers
	concat_csv = concat_csv.sort_values(['days_since_consent', 'day'])
	concat_csv['day'] = numbers
	numbers.sort(reverse = True)
	concat_csv['num'] = numbers

	print(concat_csv.T)

	file_name = "combined-PRONET-form_{0}-day1to1.csv".format(vi)
	concat_csv.to_csv(output1 + file_name, sep=',', index = False, header=True)
	print("csv saved and creating AMP-SCZ")

	## Creating AMP-SCZ files
	prescient_file = pd.read_csv(output1 + "combined-PRESCIENT-form_" + str(vi) + "-day1to1.csv")
	ampscz = pd.concat([concat_csv, prescient_file], axis=0, ignore_index=True)

	numbers = list(range(1,(len(ampscz.index) +1))) # changing day numbers to sequence
	ampscz['day'] = numbers
	ampscz = ampscz.sort_values(['days_since_consent', 'day'])
	ampscz['day'] = numbers
	numbers.sort(reverse = True)
	ampscz['num'] = numbers
	ampscz.to_csv(output1 + "combined-AMPSCZ-form_" + str(vi) + "-day1to1.csv", sep=',', index = False, header=True)

	for si in site_list:
		print(si)
		site_final = concat_csv[concat_csv['site'].str.contains(si)]
		# changing day numbers to sequence
		numbers = list(range(1,(len(site_final.index) +1))) 
		site_final.loc[:, 'day'] = numbers
		numbers.sort(reverse = True)
		site_final['num'] = numbers

		file_name = "combined-{0}-form_{1}-day1to1.csv".format(si, vi)
		site_final.to_csv(output1 + file_name, sep=',', index = False, header=True)

print("Done creating and saving month 1 to month 8 csvs")

print("Final combined Pronet csvs")
print("Screening")
print(final_csv.T.iloc[:11,:])
print("Baseline")
print(final_baseline_csv.T.iloc[:11,:])


## Creating site specific combined files for screening and baseline
for si in site_list:
	print(si)
	# baseline data
	site_final = final_baseline_csv[final_baseline_csv['site'].str.contains(si)]
	# changing day numbers to sequence
	numbers = list(range(1,(len(site_final.index) +1))) 
	site_final.loc[:, 'day'] = numbers
	numbers.sort(reverse = True)
	site_final['num'] = numbers

	file_name = "combined-{0}-form_baseline-day1to1.csv".format(si)
	site_final.to_csv(output1 + file_name, sep=',', index = False, header=True)

	# screening data
	site_scr_final = final_csv[final_csv['site'].str.contains(si)]
	# changing day numbers to sequence
	numbers = list(range(1,(len(site_scr_final.index) +1))) 
	site_scr_final['day'] = numbers
	numbers.sort(reverse = True)
	site_scr_final['num'] = numbers

	file_name = "combined-{0}-form_screening-day1to1.csv".format(si)
	site_scr_final.to_csv(output1 + file_name, sep=',', index = False, header=True)


# AMPSCZ

# loading prescient data to combine with pronet data for AMP-SCZ combined views

baseline_prescient = pd.read_csv(output1 + "combined-PRESCIENT-form_baseline-day1to1.csv")
screening_prescient = pd.read_csv(output1 + "combined-PRESCIENT-form_screening-day1to1.csv")

ampscz_screening = pd.concat([final_csv, screening_prescient],axis=0, ignore_index=True)
print("Completed concatenation for screening")
numbers = list(range(1,(len(ampscz_screening.index) +1))) # changing day numbers to sequence
ampscz_screening['day'] = numbers
ampscz_screening = ampscz_screening.sort_values(['days_since_consent', 'day'])
numbers.sort(reverse = True)
ampscz_screening['day'] = numbers
numbers.sort(reverse = True)
ampscz_screening['num'] = numbers

ampscz_baseline = pd.concat([final_baseline_csv, baseline_prescient], axis=0, ignore_index=True)
print("Completed concatenation for baseline")
numbers = list(range(1,(len(ampscz_baseline.index) +1))) # changing day numbers to sequence
ampscz_baseline['day'] = numbers
ampscz_baseline = ampscz_baseline.sort_values(['days_since_consent', 'day'])
ampscz_baseline['day'] = numbers
numbers.sort(reverse = True)
ampscz_baseline['num'] = numbers



ampscz_screening.to_csv(output1 + "combined-AMPSCZ-form_screening-day1to1.csv", sep=',', index = False, header=True)
ampscz_baseline.to_csv(output1 + "combined-AMPSCZ-form_baseline-day1to1.csv", sep=',', index = False, header=True)



print("Final combined AMPSCZ csvs")
print(ampscz_screening.T)
print(ampscz_baseline.T)







