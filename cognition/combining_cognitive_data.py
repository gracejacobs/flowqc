import sys
import numpy as np
import pandas as pd
import argparse
import json
import os
import math
from datetime import datetime
from datetime import date


# getting today's date
today = date.today()
today = today.strftime("%Y-%m-%d")


network = str(sys.argv[1])
print("Network: ", network)

output1 = "/data/predict1/data_from_nda/formqc/"

if network == "PRONET":
        ids = pd.read_csv("/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/pronet_sub_list.txt", sep= "\n", index_col = False, header = None)
else:
	ids = pd.read_csv("/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/prescient_sub_list_recent.txt", sep= "\n", index_col = False, header = None)


id_list = ids.iloc[:, 0].tolist()
print(id_list)

baseline_SPLLT_tracker = {}
baseline_NOSPLLT_tracker = {}
month2_SPLLT_tracker = {}
month2_NOSPLLT_tracker = {}
month6_SPLLT_tracker = {}
month6_NOSPLLT_tracker = {}


for id in id_list:
	print("\nID: " + id)
	site = id[0:2] #taking first part of ID
	print("Site: ", site)


	if network == "PRONET":
		sub_data = "/data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.UPENN.json".format(site, id)
		print("Participant json: ", sub_data)
	else:
		sub_data = "/data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient{0}/raw/{1}/surveys/{1}.UPENN.json".format(site, id)
		print("Participant json: ", sub_data)

	if os.path.isfile(sub_data):
		
		with open(sub_data, 'r') as f:
			sub_json = json.load(f)

		sub_data_all = pd.DataFrame.from_dict(sub_json, orient="columns")

		## need to order sessions by date and then reset index
		sub_data_all['session_date'] = pd.to_datetime(sub_data_all['session_date'])
		sub_data_all.sort_values(by='session_date')
		sub_data_all = sub_data_all.reset_index(drop=True)
		pd.set_option('display.max_rows', None)
		#print(sub_data_all.T)

		all_events = sub_data_all.index.tolist()
		print(all_events)
	
	else:
		print("COGNITIVE DATA DOES NOT EXIST")
		all_events = []

	baseline_NOSPLLT = pd.DataFrame()
	month2_NOSPLLT = pd.DataFrame()
	month6_NOSPLLT = pd.DataFrame()
	month12_NOSPLLT = pd.DataFrame()
	month18_NOSPLLT = pd.DataFrame()
	month24_NOSPLLT = pd.DataFrame()
	baseline_SPLLT = pd.DataFrame()
	month2_SPLLT = pd.DataFrame()
	month6_SPLLT = pd.DataFrame()
	month12_SPLLT = pd.DataFrame()
	month18_SPLLT = pd.DataFrame()
	month24_SPLLT = pd.DataFrame()

	for vi in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]:
		if vi in all_events:
			data = sub_data_all.loc[vi]
			#print(data)
			if data['session_battery'].startswith('ProNET') or data['session_battery'].startswith('PRESCIENT') or data['session_battery'].endswith('NOSPLLT'):
				#print(data)
				endings = ('=2', '_2', '_3', '=3')
				if vi == 0 or vi == 1 and not data['session_subid'].endswith(endings):
					baseline_NOSPLLT = pd.DataFrame(data)
					baseline_NOSPLLT = baseline_NOSPLLT.T
					baseline_NOSPLLT = baseline_NOSPLLT.reset_index(drop=True)
					#print("baseline_NOSPLLT") 
				if vi == 2 or vi == 3 or data['session_subid'].endswith('=2') or data['session_subid'].endswith('_2'):
					#print("month2_NOSPLLT")
					month2_NOSPLLT = pd.DataFrame(data)
					month2_NOSPLLT = month2_NOSPLLT.T
					month2_NOSPLLT = month2_NOSPLLT.reset_index(drop=True)
				if vi == 4 or vi == 5 or data['session_subid'].endswith('_3') or data['session_subid'].endswith('=3'):
					month6_NOSPLLT = pd.DataFrame(data)
					month6_NOSPLLT = month6_NOSPLLT.T
					month6_NOSPLLT = month6_NOSPLLT.reset_index(drop=True)
					#print("month6_NOSPLLT")
				endings = ('=2', '_2', '_3', '=3')
				if vi == 6 or vi == 7 or data['session_subid'].endswith('_4') or data['session_subid'].endswith('=4'):
					month12_NOSPLLT = pd.DataFrame(data)
					month12_NOSPLLT = month12_NOSPLLT.T
					month12_NOSPLLT = month12_NOSPLLT.reset_index(drop=True)
					#print("month12_NOSPLLT")
				if vi == 8 or vi == 9 or data['session_subid'].endswith('_5') or data['session_subid'].endswith('=5'):
					month18_NOSPLLT = pd.DataFrame(data)
					month18_NOSPLLT = month18_NOSPLLT.T
					month18_NOSPLLT = month18_NOSPLLT.reset_index(drop=True)
					#print("month18_NOSPLLT")
				if vi == 10 or vi == 11 or data['session_subid'].endswith('_6') or data['session_subid'].endswith('=6'):
					month24_NOSPLLT = pd.DataFrame(data)
					month24_NOSPLLT = month24_NOSPLLT.T
					month24_NOSPLLT = month24_NOSPLLT.reset_index(drop=True)
					#print("month24_NOSPLLT")
				#if vi == 0 or vi == 1 and not data['session_subid'].endswith(endings):
				#	baseline_NOSPLLT = data
				#	print("baseline_NOSPLLT")
			if data['session_battery'].startswith('SPLLT') or data['session_battery'].endswith('_SPLLT'):
				#print(data)
				endings = ('=2', '_2', '_3', '=3')
				if vi == 0 or vi == 1 and not data['session_subid'].endswith(endings):
					baseline_SPLLT = pd.DataFrame(data)
					baseline_SPLLT = baseline_SPLLT.T
					baseline_SPLLT = baseline_SPLLT.reset_index(drop=True)
					#print("baseline_SPLLT")
				if vi == 2 or vi == 3 or data['session_subid'].endswith('=2') or data['session_subid'].endswith('_2'):
					month2_SPLLT = pd.DataFrame(data)
					month2_SPLLT = month2_SPLLT.T
					month2_SPLLT = month2_SPLLT.reset_index(drop=True)
					#print("month2_SPLLT")
				if vi == 4 or vi == 5 or data['session_subid'].endswith('_3') or data['session_subid'].endswith('=3'):
					month6_SPLLT = pd.DataFrame(data)
					month6_SPLLT = month6_SPLLT.T
					month6_SPLLT = month6_SPLLT.reset_index(drop=True)
					#print("month6_SPLLT")
				if vi == 6 or vi == 7 or data['session_subid'].endswith('_4') or data['session_subid'].endswith('=4'):
					month12_SPLLT = pd.DataFrame(data)
					month12_SPLLT = month12_SPLLT.T
					month12_SPLLT = month12_SPLLT.reset_index(drop=True)
					#print("month12_SPLLT")
				if vi == 8 or vi == 9 or data['session_subid'].endswith('_5') or data['session_subid'].endswith('=5'):
					month18_SPLLT = pd.DataFrame(data)
					month18_SPLLT = month18_SPLLT.T
					#print("month18_SPLLT")
				if vi == 10 or vi == 11 or data['session_subid'].endswith('_6') or data['session_subid'].endswith('=6'):
					month24_SPLLT = pd.DataFrame(data)
					month24_SPLLT = month24_SPLLT.T
					month24_SPLLT = month24_SPLLT.reset_index(drop=True)
					#print("month24_SPLLT")


	print(baseline_NOSPLLT.T)
	print(baseline_SPLLT.T)
		## setting up status of data available
	if not baseline_SPLLT.empty or not baseline_NOSPLLT.empty:
		baseline_avail = 1
	else:
		baseline_avail = 0
	if not month2_SPLLT.empty or not month2_NOSPLLT.empty:
		month2_avail = 1
	else:
		month2_avail = 0
	if not month6_SPLLT.empty or not month6_NOSPLLT.empty:
		month6_avail = 1
	else:
		month6_avail = 0
	if not month12_SPLLT.empty or not month12_NOSPLLT.empty:
		month12_avail = 1
	else:
		month12_avail = 0
	if not month18_SPLLT.empty or not month18_NOSPLLT.empty:
		month18_avail = 1
	else:
		month18_avail = 0
	if not month24_SPLLT.empty or not month24_NOSPLLT.empty:
		month24_avail = 1
	else: 
		month24_avail = 0

	#print("Basline data: " + str(baseline_avail))
	#print("Month2 data: " + str(month2_avail))
	#print("Month 6 data: " + str(month6_avail))
	#print("Month 12 data: " + str(month12_avail))
	#print("Month18 data: " + str(month18_avail))
	#print("Month24 data: " + str(month24_avail))

	summary_form = pd.DataFrame()
	summary_form.at[0, "baseline_cog"] = baseline_avail
	summary_form.at[0, "month2_cog"] = month2_avail
	summary_form.at[0, "month6_cog"] = month6_avail
	summary_form.at[0, "month12_cog"] = month12_avail
	summary_form.at[0, "month18_cog"] = month18_avail
	summary_form.at[0, "month24_cog"] = month24_avail

	## setting up chart
	cog_status = 0

	if baseline_avail == 1 and month2_avail == 1:
		cog_status = 3 # baseline and month2
	if baseline_avail == 1 and month2_avail == 0 and month6_avail == 0 and month12_avail == 0 and month18_avail == 0 and month24_avail == 0:
		cog_status = 1 # only baseline
	if baseline_avail == "0" and month2_avail == 1:
		cog_status = 2 # only month 2
	if baseline_avail == 0 and month2_avail == 0 and month6_avail == "0" and month12_avail == "0" and month18_avail == "0" and month24_avail == "0":
		cog_status = 0 # no cognitive data

	if baseline_avail == 1 and month2_avail == 1 and month6_avail == 1:
		cog_status = 4 # baseline, month 2, month 6

	if baseline_avail == 1 and month2_avail == 1 and month6_avail == 1 and month12_avail == "1":
		cog_status = 5 # baseline - month12

	if baseline_avail == 1 and month2_avail == 1 and month6_avail == 1 and month12_avail == 1 and month18_avail == 1:
		cog_status = 6
	if baseline_avail == 1 and month2_avail == 1 and month6_avail == 1 and month12_avail == 1 and month18_avail == 1 and month24_avail == 1:
		cog_status = 7
	

	summary_form.at[0, "cog_status"] = cog_status	

	#print(month2_NOSPLLT.T)

	row=0
	for visit in ["baseline", "month2", "month6", "month12", "month18", "month24"]:
		#print(visit)
		#data =  vars()[str(visit) + '_NOSPLLT']
		#print(data.info())
		#print(data.T)		

		for var in ["mpract_valid_code", "spcptn90_valid_code", "er40_d_valid_code", "sfnb2_valid_code", "sfnb2_valid_code", "digsym_valid_code", "svolt_a_valid_code", "sctap_valid_code"]:

			var_vis = "{0}_{1}".format(var, visit)
			summary_form.at[0, var_vis] = ""

			data =  vars()[str(visit) + '_NOSPLLT']
                	#print(data.info())
			#print(data.T)
			if not data.empty:		
		
				var_vis = "{0}_{1}".format(var, visit)
				#print(var_vis)
			#	print(summary_form.T)
				summary_form.at[0, var_vis] = data.at[0, var]
				summary_form.at[row, var] = data.at[0, var] #also creating accumulated variable		

				if var == "digsym_valid_code" and len(data.at[0, var]) == 0:
					summary_form.at[0, var_vis] = data.at[0, "digsymb_valid_code"]
					summary_form.at[row, var] = data.at[0, "digsymb_valid_code"]

		## SPLLT
		data = vars()[str(visit) + '_SPLLT']
		var_vis = "spllt_valid_code_{0}".format(visit)
		summary_form.at[0, var_vis] = ""

		if not data.empty:
			summary_form.at[row, "session_date"] = data.at[0, "session_date"]
			if len(data.at[0, "spllt_a_valid_code"]) > 0:
				summary_form.at[0, var_vis] = data.at[0, "spllt_a_valid_code"]
				summary_form.at[row, "spllt_valid_code"] = data.at[0, "spllt_a_valid_code"]
			if len(data.at[0, "spllt_b_valid_code"]) > 0:
				summary_form.at[0, var_vis] = data.at[0, "spllt_b_valid_code"]
				summary_form.at[row, "spllt_valid_code"] = data.at[0, "spllt_b_valid_code"]
			if len(data.at[0, "spllt_c_valid_code"]) > 0:
				summary_form.at[0, var_vis] = data.at[0, "spllt_c_valid_code"]
				summary_form.at[row, "spllt_valid_code"] = data.at[0, "spllt_c_valid_code"]
			if len(data.at[0, "spllt_d_valid_code"]) > 0:
				summary_form.at[0, var_vis] = data.at[0, "spllt_d_valid_code"]
				summary_form.at[row, "spllt_valid_code"] = data.at[0, "spllt_d_valid_code"]

		row=row + 1


	#print(summary_form.T)
	summary_form = summary_form.replace('',np.nan,regex=True)
	#print(summary_form.T)


	names_dash = ['reftime','day', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']
	dpdash_all = pd.DataFrame(columns = names_dash)
	frames = [dpdash_all, summary_form]
	dpdash_main = pd.concat(frames)
	dpdash_main = dpdash_main.reset_index(drop=True)

	numbers = list(range(1, len(dpdash_main.index) + 1))
	day = max(numbers)

	#print(numbers)
	dpdash_main.loc[:, 'subjectid'] = id
	dpdash_main.loc[:, 'site'] = site
	dpdash_main.loc[:,'day'] = numbers
	dpdash_main.loc[:,'mtime'] = today
	#print(dpdash_main.T)

	dpdash_main.to_csv(output1 + site + "-" + id + "-form_cognition_summary-day1to" + str(day) + ".csv", sep=',', index = False, header=True)






