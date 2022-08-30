import sys
import numpy as np
import pandas as pd
import glob
import os


input_path = "/data/predict/data_from_nda/formqc/"
print("Input path: " + input_path + "\n")

output_path = "/data/predict/data_from_nda/formqc/"
print("Output path: " + output_path)

# read in all csvs that start with "formqc-"
#files= glob('./**/*day1to1.csv',recursive=True)
#df= pd.read_csv(files[0])

dict = pd.read_csv('/data/pnl/home/gj936/U24/Clinical_qc/flowqc/CloneOfREDCapIIYaleRecords_DataDictionary_2022-07-27.csv',
                                sep= ",",
                                index_col = False, low_memory=False)

# Getting all the form names from the data dictionary
#form_names = pd.read_csv('form_names.csv')
form_names = dict['Form Name'].unique()
#form_names = form_names.remove('psychs_p1p8_fu_draft')

### AMP-SCZ

## need to assign a participant to day

for name in form_names:
	if name not in ['psychs_p1p8_fu_draft', 'psychs_p9ac32_fu_draft']:
	#if name in ['bprs', 'assist', 'oasis']:

		print("\n" + name)
	
		for file in glob.glob(os.path.join(input_path, "YA-YA01508-form_"+name+"-day*.csv")):
			reference = pd.read_csv(file)
	
		#reference = pd.read_csv(input_path + "YA-YA01508-form_"+name+"-day1to*.csv")

		# get first line and append it 
		files = glob.glob(input_path + "*-form_" + name +"-day1*.csv")
		all_files = filter(lambda x: not x.startswith(input_path +"combined"), files)
		
		#for i in all_files:
		#	print(i)

		li = []

		num_rows = len(reference.index) 
		print("Number of rows in reference: " + str(num_rows))

		if num_rows > 0:
			all_screening = pd.DataFrame(reference.iloc[0])
		else:
			all_screening = reference
	
	
		all_screening = all_screening.T
		#print('All_screening without data: ' + all_screening)

		for filename in all_files:
			df = pd.read_csv(filename, header=0)
			print(filename)
			#print(df.iloc[::1])
			all_screening = all_screening.append(df.iloc[::1])

		# removing first rows because they are the reference
		if num_rows > 0:		
			all_screening = all_screening.iloc[num_rows: , :]
		
		#all_screening.sort_values('subjectid')
		#all_screening.iloc[natsort.index_humansorted(all_screening.subjectid)]
		all_screening[['site2', 'id_num']] = all_screening['subjectid'].str.extract('([A-Za-z]+)(\d+\.?\d*)', expand = True)
		all_screening.sort_values(['site', 'id_num'], ascending=[True, True], inplace=True)
		all_screening.drop('site2', axis=1, inplace=True)
		all_screening.drop('id_num', axis=1, inplace=True)

		all_screening = all_screening.reset_index(drop=True)
		#print(all_screening)

		# only keeping day 1 or day 2 participants
		#screening = all_screening.loc[all_screening['day'] == 1]
		#screening = screening.reset_index(drop=True)
		#baseline = all_screening.loc[all_screening['day'] != 1]
		#baseline = baseline.reset_index(drop=True)

		# Just screening data
		#numbers = list(range(1,(len(screening.index) +1))) # changing day numbers to sequence
		#screening['day'] = numbers
		#screening.to_csv(output_path+"combined_screen-AMPSCZ-form_"+name+"-day1to1.csv", sep=',', index = False, header=True)

		# Just baseline data
		#numbers = list(range(1,(len(baseline.index) +1))) # changing day numbers to sequence
		#baseline['day'] = numbers
		#baseline.to_csv(output_path+"combined_baseline-AMPSCZ-form_"+name+"-day1to1.csv", sep=',', index = False, header=True)

		# All data for all subjects
		numbers = list(range(1,(len(all_screening.index) +1))) # changing day numbers to sequence
		all_screening['day'] = numbers
		print(all_screening.T)

		all_screening.to_csv(output_path+"combined-AMPSCZ-form_"+name+"-day1to1.csv", sep=',', index = False, header=True)

		#all_screening.to_csv(output_path+"combined-YA-form_allqc-day1to1.csv", sep=',', index = False, header=True)

		all_screening.to_csv(output_path+"combined-PRONET-form_"+name+"-day1to1.csv", sep=',', index = False, header=True)













