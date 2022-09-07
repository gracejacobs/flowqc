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
site_list = ["ME"]

# list of ids to include
ids = pd.read_csv("/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/prescient_sub_list.txt", sep= "\n", index_col = False, header = None)

id_list = ids.iloc[:, 0].tolist()
#id_list = ids.iloc[:3, 0].tolist()


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

	#getting percentages for each of the forms
	percentage_file = "/data/predict/data_from_nda/formqc/{0}-{1}-percentage.csv".format(site, id)
	screening_perc = []
	baseline_perc = []

	if os.path.exists(percentage_file):
		percentages = pd.read_csv(percentage_file, sep= ",", index_col= 0)
		screening_perc = percentages.iloc[:1,:]
		baseline_perc = percentages.iloc[2:3,:]

		screening_perc = screening_perc.reset_index(drop=True)
		baseline_perc = baseline_perc.reset_index(drop=True)

	# screening
	screen = (output1 + site+"-"+id+"-screening.csv")
	if os.path.exists(percentage_file):
		sub_screening = pd.read_csv(screen)
		consent = sub_screening[sub_screening["Unnamed: 1"].isin(["chric_consent_date"])]
		consent = consent.at[0, "Variables"]
		consent = consent.split(" ")[0]
		consent = datetime.strptime(consent, "%d/%m/%Y")
		#print(consent)

		sub_screening = sub_screening.set_index('Unnamed: 1')
		del sub_screening['Unnamed: 0']
		sub_screening = sub_screening.transpose()
		#print(sub_screening)

		# setting up dpdash columns for screening
		names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime', 'days_since_consent']
		dpdash_main = pd.DataFrame(columns = names_dash)
		dpdash_main.at[0, 'subjectid'] = id
		dpdash_main.at[0, 'site'] = site
		dpdash_main.at[0, 'mtime'] = consent
		dpdash_main.at[0, 'day'] = 1

		#dpdash_main = dpdash_main.transpose()
		#print(dpdash_main)

		dpdash_main = dpdash_main.reset_index(drop=True)
		sub_screening = sub_screening.reset_index(drop=True)

		if len(screening_perc) > 0:
			frames = [dpdash_main, sub_screening, screening_perc]
		else:
			frames = [dpdash_main, sub_screening]


		dpdash_screening = pd.concat(frames, axis=1)
		dpdash_screening = dpdash_screening.loc[:,~dpdash_screening.columns.duplicated()]
		print("Screening csv: ")
		print(dpdash_screening.T)

		id_tracker["Screening_{0}".format(id)] = dpdash_screening

	# baseline
	baseline = (output1 + site+"-"+id+"-baseline.csv")

	if os.path.exists(baseline):
		sub_baseline = pd.read_csv(output1 + site+"-"+id+"-baseline.csv")
		sub_baseline = sub_baseline.set_index('Unnamed: 1')
		del sub_baseline['Unnamed: 0']
		sub_baseline = sub_baseline.transpose()
		#print(sub_baseline)
	

		# setting up dpdash columns for baseline
		names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime', 'days_since_consent']
		dpdash_main = pd.DataFrame(columns = names_dash)
		dpdash_main.at[0, 'subjectid'] = id
		dpdash_main.at[0, 'site'] = site
		dpdash_main.at[0, 'mtime'] = consent
		dpdash_main.at[0, 'day'] = 1

		#dpdash_main = dpdash_main.transpose()
		#print(dpdash_main)

		dpdash_main = dpdash_main.reset_index(drop=True)
		sub_baseline = sub_baseline.reset_index(drop=True)

		if len(baseline_perc) > 0:
			frames = [dpdash_main, sub_baseline, baseline_perc]
		else: 
			frames = [dpdash_main, sub_baseline]

		dpdash_baseline = pd.concat(frames, axis=1)

		print("Baseline csv: ")
		dpdash_baseline = dpdash_baseline.loc[:,~dpdash_baseline.columns.duplicated()]
		print(dpdash_baseline.T)

		id_baseline_tracker["Baseline_{0}".format(id)] = dpdash_baseline

	else:
		print("Baseline data doesn't exist")
		names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime', 'days_since_consent']
		dpdash_main = pd.DataFrame(columns = names_dash)
		dpdash_main.at[0, 'subjectid'] = id
		dpdash_main.at[0, 'site'] = site
		dpdash_main.at[0, 'mtime'] = consent
		dpdash_main.at[0, 'day'] = 1

		dpdash_baseline = dpdash_main

		id_baseline_tracker["Baseline_{0}".format(id)] = dpdash_baseline



## Concatenating all participant data together
# screening visit
print(len(id_tracker))
if len(id_tracker) > 0:
	final_csv = pd.concat(id_tracker,axis=0, ignore_index=True)
	#final_csv[["chrap_total"]] = final_csv[["chrap_total"]].apply(pd.to_numeric)
	#final_csv = final_csv.round({"chrap_total":1})

	numbers = list(range(1,(len(final_csv.index) +1))) # changing day numbers to sequence
	final_csv['day'] = numbers
	print("Final screening file: ")
	print(final_csv.T)
	final_csv.to_csv(output1 + "combined-PRESCIENT-form_screening-day1to1.csv", sep=',', index = False, header=True)

	## Creating site specific combined files for screening and baseline
	for si in site_list:
		print(si)
		# screening data
		site_scr_final = final_csv[final_csv['site'].str.contains(si)]
		# changing day numbers to sequence
		numbers = list(range(1,(len(site_scr_final.index) +1))) 
		site_scr_final['day'] = numbers

		file_name = "combined-{0}-form_screening-day1to1.csv".format(si)
		#site_scr_final.to_csv(output1 + file_name, sep=',', index = False, header=True)
		print("Screening site files: ")
		print(site_scr_final.T)

# baseline visit
if len(id_baseline_tracker) > 0:
	final_baseline_csv = pd.concat(id_baseline_tracker, ignore_index=True)
	numbers = list(range(1,(len(final_baseline_csv.index) +1))) # changing day numbers to sequence
	final_baseline_csv['day'] = numbers
	print("Combined baseline file: ")
	print(final_baseline_csv.T)
	final_baseline_csv.to_csv(output1 + "combined-PRESCIENT-form_baseline-day1to1.csv", sep=',', index = False, header=True)


	## Creating site specific combined files for screening and baseline
	for si in site_list:
		print(si)
		# baseline data
		site_final = final_baseline_csv[final_baseline_csv['site'].str.contains(si)]
		# changing day numbers to sequence
		numbers = list(range(1,(len(site_final.index) +1))) 
		site_final['day'] = numbers

		file_name = "combined-{0}-form_baseline-day1to1.csv".format(si)
		#site_final.to_csv(output1 + file_name, sep=',', index = False, header=True)
		print("Baseline site file: ")
		print(site_final)










