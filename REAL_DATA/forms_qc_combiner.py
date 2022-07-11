import sys
import numpy as np
import pandas as pd
import glob

input_path = "/data/predict/data_from_nda/formqc/"
print("Input path: " + input_path + "\n")

output_path = "/data/predict/data_from_nda/formqc/"
print("Output path: " + output_path)

reference = pd.read_csv('/data/predict/kcho/flow_test/formqc/formqc-YA00037-forms_qc-day1to2.csv')
#print(reference)

# read in all csvs that start with "formqc-"
#files= glob('./**/*day1to1.csv',recursive=True)
#df= pd.read_csv(files[0])

# get first line and append it 
files = glob.glob(input_path + "*-forms_qc-day1*.csv")
all_files = filter(lambda x: not x.startswith("combined"), files)
#all_files.remove("/data/predict/kcho/flow_test/formqc/formqc-combined_screen-forms_qc-day1to9999.csv")
#all_files.remove("/data/predict/kcho/flow_test/formqc/formqc-combined_baseline-forms_qc-day1to9999.csv")
#all_files.remove("/data/predict/kcho/flow_test/formqc/formqc-combined_all-forms_qc-day1to9999.csv")

li = []
all_screening = pd.DataFrame(reference.iloc[0])
all_screening = all_screening.T

for filename in all_files:
	df = pd.read_csv(filename, header=0)
	print(filename)
	#print(df.iloc[::1])
	all_screening = all_screening.append(df.iloc[::1])

# removing first two rows because there are the reference
all_screening = all_screening.iloc[2: , :]
all_screening = all_screening.reset_index(drop=True)


# only keeping day 1 participants
screening = all_screening.loc[all_screening['day'] == 1]
screening = screening.reset_index(drop=True)

baseline = all_screening.loc[all_screening['day'] == 2]
baseline = baseline.reset_index(drop=True)

# Just screening data
numbers = list(range(1,(len(screening.index) +1))) # changing day numbers to sequence
screening['day'] = numbers
screening.to_csv(output_path+"combined_screen-AMPSCZ-forms_qc-day1to1.csv", sep=',', index = False, header=True)

# Just baseline data
numbers = list(range(1,(len(baseline.index) +1)))
baseline['day'] = numbers
baseline.to_csv(output_path+"combined_baseline-AMPSCZ-forms_qc-day1to2.csv", sep=',', index = False, header=True)

# All data for all subjects
numbers = list(range(1,(len(all_screening.index) +1)))
all_screening['day'] = numbers
all_screening.to_csv(output_path+"combined-AMPSCZ-forms_qc-day1to1.csv", sep=',', index = False, header=True)

all_screening.to_csv(output_path+"combined-YA-forms_qc-day1to1.csv", sep=',', index = False, header=True)

all_screening.to_csv(output_path+"combined-PRONET-forms_qc-day1to1.csv", sep=',', index = False, header=True)













