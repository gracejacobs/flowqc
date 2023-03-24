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
#output1 = "/data/predict/data_from_nda/formqc/"
output1 = "/data/predict1/data_from_nda/formqc/"

# list of sites for site-specific combined files
site_list = ["ME", "CP", "BM", "SG", "AD", "CM", "GA", "PV", "LS"]

# list of ids to include
ids = pd.read_csv("/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/prescient_sub_list.txt", sep= "\n", index_col = False, header = None)

id_list = ids.iloc[:, 0].tolist()
#id_list.remove("ME84344")
#id_list = ids.iloc[:3, 0].tolist()


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


print("\nCombining all measures for screening to month 4 visits: ")

# for each ID going to pull out the variables
# then going to append them to each other
for id in id_list:
	print("\nID: " + id)
	last_date = id.split(" ")[0]
	print(last_date)
	id = id.split(" ")[1]
	print(id)

	site = id[0:2]
	print("Site: ", site)

	#getting percentages for each of the forms
	percentage_file = "/data/predict1/data_from_nda/formqc/{0}-{1}-percentage.csv".format(site, id)
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
	status = "0"

	if os.path.exists(percentage_file):
		print("Reading in percentage file")
		percentages = pd.read_csv(percentage_file, sep= ",", index_col= 0)
		print(percentages)
		if 1 in percentages.index.values:
			print("Screening is present")
			screening_perc = pd.DataFrame(percentages.loc[1]).transpose()
			print(screening_perc)
		if 2 in percentages.index.values:
			baseline_perc = pd.DataFrame(percentages.loc[2]).transpose()
			print(baseline_perc)
		if 3 in percentages.index.values:
			month1_perc = pd.DataFrame(percentages.loc[3]).transpose()
			print(month1_perc)
		if 4 in percentages.index.values:
			month2_perc = pd.DataFrame(percentages.loc[4]).transpose()
			print(month2_perc)
		if 5 in percentages.index.values:
			month3_perc = pd.DataFrame(percentages.loc[5]).transpose()
			print(month3_perc)
		if 6 in percentages.index.values:
			month4_perc = pd.DataFrame(percentages.loc[6]).transpose()
			print(month4_perc)
		if 7 in percentages.index.values:
			month5_perc = pd.DataFrame(percentages.loc[7]).transpose()
			print(month5_perc)
		if 8 in percentages.index.values:
			month6_perc = pd.DataFrame(percentages.loc[8]).transpose()
			print(month6_perc)
		if 9 in percentages.index.values:
			month7_perc = pd.DataFrame(percentages.loc[9]).transpose()
			print(month7_perc)
		if 10 in percentages.index.values:
			month8_perc = pd.DataFrame(percentages.loc[10]).transpose()
			print(month8_perc)


		screening_perc = screening_perc.reset_index(drop=True)
		baseline_perc = baseline_perc.reset_index(drop=True)
		month1_perc = month1_perc.reset_index(drop=True)
		month2_perc = month2_perc.reset_index(drop=True)
		month3_perc = month3_perc.reset_index(drop=True)
		month4_perc = month4_perc.reset_index(drop=True)
		month5_perc = month5_perc.reset_index(drop=True)
		month6_perc = month6_perc.reset_index(drop=True)
		month7_perc = month7_perc.reset_index(drop=True)
		month8_perc = month8_perc.reset_index(drop=True)

		# getting the visit status
		perc_check = percentages
		perc_check = perc_check.fillna(0)
		#print(perc_check)
		perc_check = perc_check.drop('informed_consent_run_sheet' , axis='columns')
		perc_check = perc_check[perc_check.index != 99]
		perc_check['Completed']= perc_check.iloc[:, 1:].sum(axis=1)
		perc_check['Total_empty'] = (perc_check == 0).sum(axis=1)
		print(perc_check.T)
		perc_check = perc_check[perc_check.Completed > 200]

		if perc_check.empty:
			status = "0"
		else:
			status = perc_check.index[-1]

			if perc_check['Total_empty'].min() > 58:
				status = "0"
	print("Visit status: " + str(status))

	# screening
	screen = (output1 + site+"-"+id+"-screening.csv")
	if os.path.exists(screen):
		sub_screening = pd.read_csv(screen)
		consent = sub_screening[sub_screening["Unnamed: 1"].isin(["chric_consent_date"])]
		consent = consent.at[0, "Variables"]
		consent = str(consent)
		consent = consent.split(" ")[0]
		consent = datetime.strptime(consent, "%d/%m/%Y")
		consent = str(consent)
		consent = consent.split(" ")[0]

	else:
		consent = today

	# setting up dpdash columns for screening
	names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime', 'days_since_consent', 'weeks_since_consent', 'visit_status', 'file_updated']
	dpdash_main = pd.DataFrame(columns = names_dash)
	dpdash_main.at[0, 'subjectid'] = id
	dpdash_main.at[0, 'site'] = site
	dpdash_main.at[0, 'mtime'] = consent
	dpdash_main.at[0, 'day'] = 1
	dpdash_main.at[0, 'days_since_consent'] = days_between(consent, today)
	dpdash_main.at[0, 'visit_status'] = status
	dpdash_main.at[0, 'file_updated'] = last_date
	dpdash_main.at[0, 'weeks_since_consent'] = round(dpdash_main.at[0, 'days_since_consent'] / 7)
	time_since_update = days_between(today, last_date)
	if time_since_update > 49 and status != "0" and status != "99": # It's been over 7 weeks since we received data
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
	if status == 99:
		dpdash_main.at[0, 'visit_status_string'] = "removed"

	print("Visit Status: ")
	print(dpdash_main.at[0, 'visit_status_string'])

	#dpdash_main = dpdash_main.transpose()
	#print("Printing dpdash main")
	#print(dpdash_main.T)

	status_removed = "0"
	if os.path.exists(screen):
		sub_screening = sub_screening.set_index('Unnamed: 1')
		del sub_screening['Unnamed: 0']
		sub_screening = sub_screening.transpose()
		#print("Printing sub_screening...")
		#print(sub_screening.T)
		#print(screening_perc.T)

		dpdash_main = dpdash_main.reset_index(drop=True)
		sub_screening = sub_screening.reset_index(drop=True)

		if len(screening_perc) > 0:
			print("Combining frames including percentages")
			frames = [dpdash_main, sub_screening, screening_perc]
		else:
			frames = [dpdash_main, sub_screening]

		dpdash_screening = pd.concat(frames, axis=1)
		dpdash_screening = dpdash_screening.loc[:,~dpdash_screening.columns.duplicated()]
		print("Screening csv: ")
		#print(dpdash_screening.T.iloc[:10 , :14])
		
		if "chrmiss_withdrawn" in dpdash_screening and dpdash_screening.at[0, "chrmiss_withdrawn"] == '1':
			status_removed = "1"
			dpdash_screening.at[0, 'visit_status_string'] = "removed"
		if "chrmiss_discon" in dpdash_screening and dpdash_screening.at[0, "chrmiss_discon"] == '1':
			status_removed = "1"
			dpdash_screening.at[0, 'visit_status_string'] = "removed"
		dpdash_screening[0, "status_removed"] = status_removed
		print(dpdash_screening)

		id_tracker["Screening_{0}".format(id)] = dpdash_screening
	
	# baseline
	baseline = (output1 + site+"-"+id+"-baseline.csv")

	if os.path.exists(baseline):
		sub_baseline = pd.read_csv(output1 + site+"-"+id+"-baseline.csv")
		sub_baseline = sub_baseline.set_index('Unnamed: 1')
		del sub_baseline['Unnamed: 0']
		sub_baseline = sub_baseline.transpose()

		print("Visit Status: ")
		print(dpdash_main.at[0, 'visit_status_string'])
		#dpdash_main = dpdash_main.transpose()
		#print("baseline dpdash main")		
		#print(dpdash_main.T.iloc[:10 , :14])

		dpdash_main = dpdash_main.reset_index(drop=True)
		sub_baseline = sub_baseline.reset_index(drop=True)

		if len(baseline_perc) > 0:
			frames = [dpdash_main, sub_baseline, baseline_perc]
		else: 
			frames = [dpdash_main, sub_baseline]

		dpdash_baseline = pd.concat(frames, axis=1)

		print("Baseline csv: ")
		dpdash_baseline = dpdash_baseline.loc[:,~dpdash_baseline.columns.duplicated()]
		print(dpdash_baseline.T.iloc[:10 , :14])

		if "chrmiss_withdrawn" in dpdash_baseline and dpdash_baseline.at[0, "chrmiss_withdrawn"] == '1':
			status_removed = "1"
			dpdash_baseline.at[0, 'visit_status_string'] = "removed"
		if "chrmiss_discon" in dpdash_baseline and dpdash_baseline.at[0, "chrmiss_discon"] == '1':
			status_removed = "1"
			dpdash_baseline.at[0, 'visit_status_string'] = "removed"

		dpdash_baseline[0, "status_removed"] = status_removed
		print(dpdash_baseline)

		id_baseline_tracker["Baseline_{0}".format(id)] = dpdash_baseline

	else:
		print("Baseline data doesn't exist")
		dpdash_baseline = dpdash_main
		id_baseline_tracker["Baseline_{0}".format(id)] = dpdash_baseline


	# month1
	for vi in ["month1", "month2", "month3", "month4", "month5", "month6", "month7", "month8"]:
		print("Month " + vi)

		file = (output1 + site+"-"+id+"-" + str(vi) +".csv")
		visit_tracker = vars()['id_' + str(vi) + '_tracker']
		visit_perc = vars()[str(vi) + '_perc']

		if os.path.exists(file):
			print("file exists")
			sub_event = pd.read_csv(file)
			sub_event = sub_event.set_index('Unnamed: 1')
			del sub_event['Unnamed: 0']
			sub_event = sub_event.transpose()
			print(sub_event)
			sub_event = sub_event.loc[:,~sub_event.columns.duplicated()]
			print(sub_event)

			print("Visit Status: " + dpdash_main.at[0, 'visit_status_string'])
			#print(dpdash_main.at[0, 'visit_status_string'])
			#dpdash_main = dpdash_main.transpose()
			#print("dpdash main")		
			#print(dpdash_main.T.iloc[:10 , :14])

			dpdash_main = dpdash_main.reset_index(drop=True)
			sub_event = sub_event.reset_index(drop=True)

			if len(visit_perc) > 0:
				frames = [dpdash_main, sub_event, visit_perc]
			else: 
				frames = [dpdash_main, sub_event]

			dpdash_event = pd.concat(frames, axis=1)

			# remove duplicated columns
			dpdash_event = dpdash_event.loc[:,~dpdash_event.columns.duplicated()]
			#print(dpdash_event.T.iloc[:25 , :25])

			if "chrmiss_withdrawn" in dpdash_event and dpdash_event.at[0, "chrmiss_withdrawn"] == '1':
				status_removed = "1"
				dpdash_event.at[0, 'visit_status_string'] = "removed"
			if "chrmiss_discon" in dpdash_event and dpdash_event.at[0, "chrmiss_discon"] == '1':
				status_removed = "1"
				dpdash_event.at[0, 'visit_status_string'] = "removed"

			dpdash_event.at[0, "status_removed"] = status_removed
			#print(dpdash_event.T.iloc[:25 , :25])

			#print("{0}_{1}".format(vi, id))
			visit_tracker["{0}_{1}".format(vi, id)] = dpdash_event

		else:
			print("Month" + str(vi) + " data doesn't exist")
			dpdash_event = dpdash_main
			#print(dpdash_event.iloc[:5,:5])
			visit_tracker["{0}_{1}".format(vi, id)] = dpdash_event



## Concatenating all participant data together
# screening visit
#print(len(id_tracker))
if len(id_tracker) > 0:
	final_csv = pd.concat(id_tracker,axis=0, ignore_index=True)
	final_csv.dropna(subset=['subjectid'], inplace=True)

	numbers = list(range(1,(len(final_csv.index) +1))) # changing day numbers to sequence
	print("Length of csv: " + str(numbers)) 
	final_csv['day'] = numbers
	
	# reordering based on days since consent
	final_csv = final_csv.sort_values(['days_since_consent', 'day'])
	final_csv['day'] = numbers
	numbers.sort(reverse = True)
	final_csv['num'] = numbers

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
		numbers.sort(reverse = True)
		site_scr_final['num'] = numbers

		file_name = "combined-{0}-form_screening-day1to1.csv".format(si)
		site_scr_final.to_csv(output1 + file_name, sep=',', index = False, header=True)
		#print("Screening site files: ")
		#print(site_scr_final.T)


	else:
		print("No data for screening")

# baseline visit
if len(id_baseline_tracker) > 0:
	final_baseline_csv = pd.concat(id_baseline_tracker, ignore_index=True)
	final_baseline_csv.dropna(subset=['subjectid'], inplace=True)
	numbers = list(range(1,(len(final_baseline_csv.index) +1))) # changing day numbers to sequence
	final_baseline_csv['day'] = numbers
	
	# reordering based on days since consent
	final_baseline_csv = final_baseline_csv.sort_values(['days_since_consent', 'day'])
	#print(final_baseline_csv.T.iloc[:14 , :5])
	final_baseline_csv['day'] = numbers
	numbers.sort(reverse = True)
	final_baseline_csv['num'] = numbers

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
		numbers.sort(reverse = True)
		site_final['num'] = numbers

		file_name = "combined-{0}-form_baseline-day1to1.csv".format(si)
		site_final.to_csv(output1 + file_name, sep=',', index = False, header=True)
		#print("Baseline site file: ")
		#print(site_final)


	else:
		print("No data for baseline")



# month1 visit
for vi in ["1", "2", "3", "4", "5", "6", "7", "8"]:
	print("Combining and saving data for month " + vi)
	tracker_name = vars()['id_month' + str(vi) + '_tracker']
	print("Length: " + str(len(tracker_name)))
	print(tracker_name)

	if len(tracker_name) > 0:
		final_event_csv = pd.concat(tracker_name, ignore_index=True)
		#print("Before extra subjects are dropped")
		#print(final_event_csv)
		#final_event_csv.dropna(subset=['subjectid'], inplace=True)
		#final_event_csv = final_event_csv.drop_duplicates(subset=['subjectid'])
		#print("Before extra subjects are dropped")
		#print(final_event_csv)
		numbers = list(range(1,(len(final_event_csv.index) +1))) # changing day numbers to sequence
		final_event_csv['day'] = numbers
	
		# reordering based on days since consent
		final_event_csv = final_event_csv.sort_values(['days_since_consent', 'day'])
		final_event_csv['day'] = numbers
		numbers.sort(reverse = True)
		final_event_csv['num'] = numbers

		print("Combined month" + str(vi) + " file: ")
		print(final_event_csv.T)
		final_event_csv.to_csv(output1 + "combined-PRESCIENT-form_month" + str(vi) + "-day1to1.csv", sep=',', index = False, header=True)

		## Creating site specific combined files for screening and baseline
		print("INDIVIUAL SITES")
		for si in site_list:
			print(si)
			site_final = final_event_csv[final_event_csv['site'].str.contains(si)]
			# changing day numbers to sequence
			numbers = list(range(1,(len(site_final.index) +1))) 
			site_final['day'] = numbers
			numbers.sort(reverse = True)
			site_final['num'] = numbers

			file_name = "combined-{0}-form_month{1}-day1to1.csv".format(si, vi)
			site_final.to_csv(output1 + file_name, sep=',', index = False, header=True)

	else:
		print("No data for month " + str(vi))




