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

# output folder
output1 = "/data/predict/data_from_nda/formqc/"

# list of sites for site-specific combined files
site_list = ["AD", "CA", "SD", "SF", "HA", "YA", "LA", "WU", "PI", "PA", "OR", "NN", "IR", "NL", "NN", "NC", "TE", "MT"]

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

print("\nCombining all measures for screening and baseline visits: ")

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

	sub_data = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)

	#print(sub_data)
	with open(sub_data, 'r') as f:
		sub_json = json.load(f)

	sub_data_all = pd.DataFrame.from_dict(sub_json, orient="columns")
	#replacing empty cells with NaN
	sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)
	sub_data_all.dropna(axis=1, how='all', inplace=True)

	#print(sub_data_all)
	
	all_events = sub_data_all['redcap_event_name'].unique()
	screening = [i for i in all_events if i.startswith('screening_')]
	screening = screening[0]

	baseline = [i for i in all_events if i.startswith('baseline_')]
	if baseline == []:
		baseline = ['month_1_arm_1']
		baseline = baseline[0]
	else:
		baseline = baseline[0]

	month1 = [i for i in all_events if i.startswith('month_1_')]
	if month1 == []:
		month1 = []
	else:
		month1 = month1[0]

	month2 = [i for i in all_events if i.startswith('month_2_')]
	if month2 == []:
		month2 = []
	else:
		month2 = month2[0]

	month3 = [i for i in all_events if i.startswith('month_3_')]
	if month3 == []:
		month3 = []
	else:
		month3 = month3[0]

	month4 = [i for i in all_events if i.startswith('month_4_')]
	if month4 == []:
		month4 = []
	else:
		month4 = month4[0]

	# setting up screening
	sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([screening])]
	sub_data = sub_data.reset_index(drop=True)
	#print(sub_data)
	
	# setting up baseline
	sub_data_baseline = sub_data_all[sub_data_all['redcap_event_name'].isin([baseline])]
	sub_data_baseline = sub_data_baseline.reset_index(drop=True)
	#print(sub_data_baseline)

	# setting up month 1, 2, 3, 4
	# creating empty dataframes if timepoint doesn't exist
	if month1 != []:
		sub_data_month1 = sub_data_all[sub_data_all['redcap_event_name'].isin([month1])]
		sub_data_month1 = sub_data_month1.reset_index(drop=True)
	else: 
		sub_data_month1 = pd.DataFrame()
	print(sub_data_month1)
	if month2 != []:
		sub_data_month2 = sub_data_all[sub_data_all['redcap_event_name'].isin([month2])]
		sub_data_month2 = sub_data_month2.reset_index(drop=True)
	else: 
		sub_data_month2 = pd.DataFrame()
	print(sub_data_month2)
	if month3 != []:
		sub_data_month3 = sub_data_all[sub_data_all['redcap_event_name'].isin([month3])]
		sub_data_month3 = sub_data_month3.reset_index(drop=True)
	else: 
		sub_data_month3 = pd.DataFrame()
	print(sub_data_month3)
	if month4 != []:
		sub_data_month4 = sub_data_all[sub_data_all['redcap_event_name'].isin([month4])]
		sub_data_month4 = sub_data_month4.reset_index(drop=True)
	else: 
		sub_data_month4 = pd.DataFrame()
	print(sub_data_month4)
	
	# could add all the interview dates and entry dates here
	# find the difference between them, name them based on the form	

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
	sub_data.at["included_excluded", 'Variables'] = np.nan

	if 'chrcrit_included' in sub_data and pd.notna(sub_data.at[0, "chrcrit_included"]) and sub_data.loc[0, "chrcrit_included"] == "1":
		sub_data.at[0, "included_excluded"] = 1
	if "chrcrit_excluded" in sub_data and pd.notna(sub_data.loc[0, "chrcrit_excluded"]) and sub_data.loc[0, "chrcrit_excluded"] == "1":
		sub_data.at[0, "included_excluded"] = 0

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
		

	#getting percentages for each of the forms
	percentage_file = "/data/predict/data_from_nda/formqc/{0}-{1}-percentage.csv".format(site, id)
	status = "0"
	if os.path.exists(percentage_file):
		percentages = pd.read_csv(percentage_file, sep= ",", index_col= 0)
		screening_perc = pd.DataFrame(percentages.loc[screening])
		baseline_perc = pd.DataFrame(percentages.loc[baseline])
		month1_perc = pd.DataFrame(percentages.loc[month1])
		month2_perc = pd.DataFrame(percentages.loc[month2])
		month3_perc = pd.DataFrame(percentages.loc[month3])
		month4_perc = pd.DataFrame(percentages.loc[month4])

		screening_perc = screening_perc.transpose()
		baseline_perc = baseline_perc.transpose()
		month1_perc = month1_perc.transpose()
		month2_perc = month2_perc.transpose()
		month3_perc = month3_perc.transpose()
		month4_perc = month4_perc.transpose()

		screening_perc = screening_perc.reset_index(drop=True)
		baseline_perc = baseline_perc.reset_index(drop=True)
		month1_perc = month1_perc.reset_index(drop=True)
		month2_perc = month2_perc.reset_index(drop=True)
		month3_perc = month3_perc.reset_index(drop=True)
		month4_perc = month4_perc.reset_index(drop=True)

		# getting the visit status
		perc_check = percentages
		perc_check = perc_check.drop('informed_consent_run_sheet' , axis='columns')
		perc_check = perc_check[perc_check.index.str.contains('floating')==False]

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
	
	if "chrmiss_withdrawn" in sub_data and sub_data.at[0, "chrmiss_withdrawn"] == '1':
		status = 99
	if "chrmiss_discon" in sub_data and sub_data.at[0, "chrmiss_discon"] == '1':
		status = 99

	print("Visit status: " + str(status))
		

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
	dpdash_main.at[0, 'file_updated'] = last_date

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
	if status == 99:
		dpdash_main.at[0, 'visit_status_string'] = "removed"

	print("Visit Status: ")
	print(dpdash_main.at[0, 'visit_status_string'])

	dpdash_main = dpdash_main.reset_index(drop=True)

	# concatenating screening and dpdash main
	sub_data = sub_data.reset_index(drop=True)

	frames = [dpdash_main, sub_data, screening_perc]
	dpdash_full = pd.concat(frames, axis=1)

	dpdash_full.dropna(how='all', axis=0, inplace=True)
	#print("Screening csv for participant: ")
	#print(dpdash_full.T)
	
	id_tracker["Screening_{0}".format(id)] = dpdash_full

	# concatenating dpdash main and baseline
	sub_data_baseline = sub_data_baseline.reset_index(drop=True)

	frames = [dpdash_main, sub_data_baseline, baseline_perc]
	dpdash_full = pd.concat(frames, axis=1)
	dpdash_full.dropna(how='all', axis=0, inplace=True)
	#print("Baseline csv for participant: ")
	#print(dpdash_full.T)

	id_baseline_tracker["Baseline_{0}".format(id)] = dpdash_full

	# concatenating dpdash main and month1, month2, month3, month4
	#sub_data_month1 = sub_data_month1.reset_index(drop=True)
	sub_data_month1 = sub_data_month1.reset_index(drop=True)

	frames = [dpdash_main, sub_data_month1, month1_perc]
	dpdash_full = pd.concat(frames, axis=1)

	print("Month1 csv for participant: ")
	dpdash_full.dropna(how='all', axis=0, inplace=True)
	print(dpdash_full.T)

	id_month1_tracker["Month1_{0}".format(id)] = dpdash_full

	# concatenating dpdash main and month2, month3, month4
	frames = [dpdash_main, sub_data_month2, month2_perc]
	dpdash_full = pd.concat(frames, axis=1)
	dpdash_full.dropna(how='all', axis=0, inplace=True)
	id_month2_tracker["Month2_{0}".format(id)] = dpdash_full

	frames = [dpdash_main, sub_data_month3, month3_perc]
	dpdash_full = pd.concat(frames, axis=1)
	dpdash_full.dropna(how='all', axis=0, inplace=True)
	id_month3_tracker["Month3_{0}".format(id)] = dpdash_full

	frames = [dpdash_main, sub_data_month4, month4_perc]
	dpdash_full = pd.concat(frames, axis=1)
	dpdash_full.dropna(how='all', axis=0, inplace=True)
	id_month4_tracker["Month4_{0}".format(id)] = dpdash_full


## Concatenating all participant data together
# screening visit
final_csv = pd.concat(id_tracker, ignore_index=True)

if "chrap_total" in final_csv:
	final_csv[["chrap_total"]] = final_csv[["chrap_total"]].apply(pd.to_numeric)
	final_csv = final_csv.round({"chrap_total":1})

print("Creating and saving screening and baseline csvs")
numbers = list(range(1,(len(final_csv.index) +1))) # changing day numbers to sequence
final_csv['day'] = numbers

final_csv = final_csv.sort_values(['days_since_consent', 'day'])
final_csv['day'] = numbers
	
# baseline visit
final_baseline_csv = pd.concat(id_baseline_tracker, ignore_index=True)
numbers = list(range(1,(len(final_baseline_csv.index) +1))) # changing day numbers to sequence
final_baseline_csv['day'] = numbers

final_baseline_csv = final_baseline_csv.sort_values(['days_since_consent', 'day'])
final_baseline_csv['day'] = numbers

###### Saving combined csvs
final_csv.to_csv(output1 + "combined-PRONET-form_screening-day1to1.csv", sep=',', index = False, header=True)
final_baseline_csv.to_csv(output1 + "combined-PRONET-form_baseline-day1to1.csv", sep=',', index = False, header=True)

for vi in ["month1", "month2", "month3", "month4"]:
	
	print(vi)
	tracker_name = vars()['id_' + str(vi) + '_tracker']
	concat_csv = pd.concat(tracker_name, ignore_index=True)

	numbers = list(range(1,(len(concat_csv.index) +1))) # changing day numbers to sequence
	concat_csv['day'] = numbers
	concat_csv = concat_csv.sort_values(['days_since_consent', 'day'])
	concat_csv['day'] = numbers
	print(concat_csv.T)

	file_name = "combined-PRONET-form_{0}-day1to1.csv".format(vi)
	concat_csv.to_csv(output1 + file_name, sep=',', index = False, header=True)
	print("csv saved")

print("Done creating and saving month 1 to month 4 csvs")

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
	site_final['day'] = numbers

	file_name = "combined-{0}-form_baseline-day1to1.csv".format(si)
	site_final.to_csv(output1 + file_name, sep=',', index = False, header=True)

	# screening data
	site_scr_final = final_csv[final_csv['site'].str.contains(si)]
	# changing day numbers to sequence
	numbers = list(range(1,(len(site_scr_final.index) +1))) 
	site_scr_final['day'] = numbers

	file_name = "combined-{0}-form_screening-day1to1.csv".format(si)
	site_scr_final.to_csv(output1 + file_name, sep=',', index = False, header=True)

	# month1 data
	#site_scr_final = final_month1_csv[final_month1_csv['site'].str.contains(si)]
	# changing day numbers to sequence
	#numbers = list(range(1,(len(site_scr_final.index) +1))) 
	#site_scr_final['day'] = numbers

	#file_name = "combined-{0}-form_month1-day1to1.csv".format(si)
	#site_scr_final.to_csv(output1 + file_name, sep=',', index = False, header=True)


# AMPSCZ

# loading prescient data

baseline_prescient = pd.read_csv(output1 + "combined-PRESCIENT-form_baseline-day1to1.csv")
screening_prescient = pd.read_csv(output1 + "combined-PRESCIENT-form_screening-day1to1.csv")

ampscz_screening = pd.concat([final_csv, screening_prescient],axis=0, ignore_index=True)
print("Completed concatenation for screening")
numbers = list(range(1,(len(ampscz_screening.index) +1))) # changing day numbers to sequence
ampscz_screening['day'] = numbers
ampscz_screening = ampscz_screening.sort_values(['days_since_consent', 'day'])
ampscz_screening['day'] = numbers

ampscz_baseline = pd.concat([final_baseline_csv, baseline_prescient], axis=0, ignore_index=True)
print("Completed concatenation for baseline")
numbers = list(range(1,(len(ampscz_baseline.index) +1))) # changing day numbers to sequence
ampscz_baseline['day'] = numbers
ampscz_baseline = ampscz_baseline.sort_values(['days_since_consent', 'day'])
ampscz_baseline['day'] = numbers

ampscz_screening.to_csv(output1 + "combined-AMPSCZ-form_screening-day1to1.csv", sep=',', index = False, header=True)
ampscz_baseline.to_csv(output1 + "combined-AMPSCZ-form_baseline-day1to1.csv", sep=',', index = False, header=True)

print("Final combined AMPSCZ csvs")
print(ampscz_screening.T)
print(ampscz_baseline.T)







