import sys
import numpy as np
import pandas as pd

# Requires two arguements - ID and Site
id = str(sys.argv[1])
print("ID: ", id)

site=str(sys.argv[2])
print("Site: ", site)

output_path = "/data/predict/kcho/flow_test/formqc/"
print("Output path: " + output_path)

#reading in individual event csvs
screening = pd.read_csv("Pronet_status/screening_arm_1"+"-"+site+"-"+id+"-formscheck.csv")
baseline = pd.read_csv("Pronet_status/baseline_arm_1"+"-"+site+"-"+id+"-formscheck.csv")

# appending event csvs
dpdash = screening.append(baseline)
dpdash = dpdash.reset_index(drop=True)
dpdash.at[0,'day']='1' #making sure the first day is labeled as 1
day = dpdash.at[1,'day']
day = str(day)
# can reorder variables later if clearer

# getting column names
cols = list(dpdash.columns)
to_remove = {'day', 'reftime', 'timeofday', 'weekday', 'site', 'mtime'}
cols = [e for e in cols if e not in to_remove]
cols = pd.DataFrame(columns = cols)

# saving the csv
dpdash.to_csv("Pronet_status/formqc-"+id+"-percent-day1to"+day+".csv", sep=',', index = False, header=True)

dpdash.to_csv(output_path+"formqc-"+id+"-percent-day1to"+day+".csv", sep=',', index = False, header=True)

# saving column names
cols.to_csv("Column_names.csv", sep=",", index=False, header=True)

#column_names = pd.read_csv("Column_names.csv")
#cols = list(column_names.columns)
#print(cols)









