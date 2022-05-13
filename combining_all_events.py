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

# saving the csv
dpdash.to_csv("Pronet_status/formqc-"+site+"-"+id+"-formscheck-day1to"+day+".csv", sep=',', index = False, header=True)

dpdash.to_csv(output_path+"formqc-"+site+"-"+id+"-formscheck-day1to"+day+".csv", sep=',', index = False, header=True)







