import sys
import os
import numpy as np
import pandas as pd
import argparse
import json

id = str(sys.argv[1])
print("ID: ", id)

site=str(sys.argv[2])
print("Site: ", site)

# setting up output directory for the id
output_path = "/data/predict/kcho/flow_test/formqc/"
path = os.path.join(output_path, id)

# Check whether the specified path exists or not
isExist = os.path.exists(path)

if not isExist:
  os.makedir(path)

event_list = ["screening_arm_1", "baseline_arm_1"]

sub_data = "/data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pronet{0}/raw/{1}/surveys/{1}.Pronet.json".format(site, id)
print("Participant json: ", sub_data)

with open(sub_data, 'r') as f:
	json = json.load(f)

sub_data_all = pd.DataFrame.from_dict(json, orient="columns")
#replacing empty cells with NaN
sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace('', np.nan)

# Opening data dictionary
#dict = pd.read_csv('AMPSCZFormRepository_DataDictionary_2022-04-02.csv',
dict = pd.read_csv('CloneOfREDCapIIYaleRecords_DataDictionary_2022-05-11.csv',
                                sep= ",",
                                index_col = False, low_memory=False)

# Getting all the form names from the data dictionary
form_names = dict['Form Name'].unique()

# Subsetting data based on event
#events = sub_data.redcap_event_name.unique()
for event in event_list:
	print("Event: " + event)
	sub_data = sub_data_all[sub_data_all['redcap_event_name'].isin([event])]
	sub_data = sub_data.reset_index(drop=True)

	### Looping through each of the variables in each of the forms to print the variables to a csv

	col=0
	for name in form_names:
		print(name)
		col=col+1
		form = dict.loc[dict['Form Name'] == name]
		form_vars = form['Variable / Field Name'].tolist()

		form_info = pd.DataFrame(columns = ['Value'], index = form_vars).rename_axis('Variable', axis=1)

		for var in form_vars:
			if var in sub_data:
				form_info.at[var, 'Value'] = sub_data.at[0, var]

		form_info = form_info.dropna(axis=0)			
		#print(form_info)

		if len(form_info):
			form_info.to_csv(path+'/'+id+'_'+event+'_'+name+".csv", sep=',', index = True, header=True, index_label='Variable')




