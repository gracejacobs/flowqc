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
site_list = ["YA", "LA", "WU", "PI", "PA", "OR", "NN"]

# list of ids to include
ids = pd.read_csv("/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/pronet_sub_list_chr.txt", sep= "\n", index_col = False, header = None)

id_list = ids.iloc[:, 0].tolist()
#id_list = ids.iloc[1:2, 0].tolist()

## Getting screening and baseline variables
screening_vars = pd.read_csv("/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/screening_variables.csv", sep= ", ", index_col = False)
screening_vars = screening_vars.columns.tolist()

baseline_vars = pd.read_csv("/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/baseline_variables.csv", sep= ", ", index_col = False)
baseline_vars = baseline_vars.columns.tolist()

# Printing out IDs
print("ID List: ")
for i in id_list:
	print(i)

id_tracker = {}
id_baseline_tracker = {}

print("\nCombining all measures for screening and baseline visits: ")

# for each ID going to pull out the variables
# then going to append them to each other
for id in id_list:
	print("\nID: " + id)
	site = id[0:2]
	print("Site: ", site)

	sub_data = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)

	print(sub_data)
	with open(sub_data, 'r') as f:
		sub_json = json.load(f)

	sub_data_all = pd.DataFrame.from_dict(sub_json, orient="columns")
	#replacing empty cells with NaN
	sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)
	
	all_events = sub_data_all['redcap_event_name'].unique()
	screening = [i for i in all_events if i.startswith('screening_')]
	screening = screening[0]
	baseline = [i for i in all_events if i.startswith('baseline_')]
	baseline = baseline[0]

	#print("Screening visit: " + screening)
	#sub_data = sub_data_all['redcap_event_name'].str.startswith("screening", na = False)
	sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([screening])]
	sub_data = sub_data.reset_index(drop=True)

	sub_data_baseline = sub_data_all[sub_data_all['redcap_event_name'].isin([baseline])]
	sub_data_baseline = sub_data_baseline.reset_index(drop=True)

	form_info = pd.DataFrame(columns = ['Variables'])
	form_info_baseline = pd.DataFrame(columns = ['Variables'])

	for var in screening_vars:
		if var in sub_data:
			form_info.at[var, 'Variables'] = sub_data.at[0, var]

	for var in baseline_vars:
		if var in sub_data_baseline:
			form_info_baseline.at[var, 'Variables'] = sub_data_baseline.at[0, var]

	form_info = form_info.transpose()
	form_info_baseline = form_info_baseline.transpose()


	#getting percentages for each of the forms
	percentage_file = "/data/predict/data_from_nda/formqc/{0}-{1}-percentage.csv".format(site, id)

	if os.path.exists(percentage_file):
		percentages = pd.read_csv(percentage_file, sep= ",", index_col= 0)
		screening_perc = pd.DataFrame(percentages.loc[screening])
		baseline_perc = pd.DataFrame(percentages.loc[baseline])

		screening_perc = screening_perc.transpose()
		baseline_perc = baseline_perc.transpose()

		screening_perc = screening_perc.reset_index(drop=True)
		baseline_perc = baseline_perc.reset_index(drop=True)

	print(screening_perc)
	print(baseline_perc)

	# setting up dpdash columns for screening
	names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime', 'days_since_consent']
	dpdash_main = pd.DataFrame(columns = names_dash)
	dpdash_main.at[screening, 'subjectid'] = id
	dpdash_main.at[screening, 'site'] = site
	dpdash_main.at[screening, 'mtime'] = sub_data.at[0, "chric_consent_date"]
	dpdash_main.at[screening, 'day'] = 1
	dpdash_main.at[screening, 'days_since_consent'] = days_between(sub_data.at[0, "chric_consent_date"], today)

	dpdash_main = dpdash_main.reset_index(drop=True)
	form_info = form_info.reset_index(drop=True)

	frames = [dpdash_main, form_info, screening_perc]
	dpdash_full = pd.concat(frames, axis=1)
	print("Screening csv: ")
	print(dpdash_full.T)
	
	id_tracker["Screening_{0}".format(id)] = dpdash_full

	# setting up dpdash columns for baseline
	names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime', 'days_since_consent']
	dpdash_main = pd.DataFrame(columns = names_dash)
	dpdash_main.at[baseline, 'subjectid'] = id
	dpdash_main.at[baseline, 'site'] = site
	dpdash_main.at[baseline, 'mtime'] = sub_data.at[0, "chric_consent_date"] 
	#should change this to an int date
	dpdash_main.at[baseline, 'day'] = 1
	dpdash_main.at[baseline, 'days_since_consent'] = days_between(sub_data.at[0, "chric_consent_date"], today)

	dpdash_main = dpdash_main.reset_index(drop=True)
	form_info_baseline = form_info_baseline.reset_index(drop=True)

	frames = [dpdash_main, form_info_baseline, baseline_perc]
	dpdash_full = pd.concat(frames, axis=1)

	print("Baseline csv: ")
	print(dpdash_full.T)

	id_baseline_tracker["Baseline_{0}".format(id)] = dpdash_full


## Concatenating all participant data together
# screening visit
final_csv = pd.concat(id_tracker, ignore_index=True)
final_csv[["chrap_total"]] = final_csv[["chrap_total"]].apply(pd.to_numeric)
final_csv = final_csv.round({"chrap_total":1})

numbers = list(range(1,(len(final_csv.index) +1))) # changing day numbers to sequence
final_csv['day'] = numbers
	
# baseline visit
final_baseline_csv = pd.concat(id_baseline_tracker, ignore_index=True)
numbers = list(range(1,(len(final_baseline_csv.index) +1))) # changing day numbers to sequence
final_baseline_csv['day'] = numbers

## Saving combined csvs
# AMPSCZ and Pronet
final_csv.to_csv(output1 + "combined-AMPSCZ-form_screening-day1to1.csv", sep=',', index = False, header=True)
final_baseline_csv.to_csv(output1 + "combined-AMPSCZ-form_baseline-day1to1.csv", sep=',', index = False, header=True)

print("Final combined AMPSCZ csvs")
print(final_csv.T)
print(final_baseline_csv.T)

final_csv.to_csv(output1 + "combined-PRONET-form_screening-day1to1.csv", sep=',', index = False, header=True)
final_baseline_csv.to_csv(output1 + "combined-PRONET-form_baseline-day1to1.csv", sep=',', index = False, header=True)

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










