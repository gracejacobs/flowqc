import sys
import numpy as np
import pandas as pd
import argparse
import json
from datetime import datetime

def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

### Requires two arguements - ID number (YA00009), site (e.g., YA) 
# ME57953

id = str(sys.argv[1])
print("ID: ", id)

site=str(sys.argv[2])
print("Site: ", site)

output_path = "/data/predict/kcho/flow_test/formqc/"
print("Output path: " + output_path)

bprs = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_BPRS.csv")

cdss = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_CDSS.csv")

coenrollment = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_Coenrollment.csv")

demos = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_Demographics.csv")

health = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_HealthConditions.csv")

inclusion = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_InclusionExclusionCriteriaReview.csv")

lifetime = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_LifetimeAP.csv")

missing = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_MissingData.csv")

pgis = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_PGIS.csv")

premorbidiq = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_PremorbidIQ.csv")

promis = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_PromisSD.csv")

recruitment = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_RecruitmentSource.csv")

inclusion = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_InclusionExclusionCriteriaReview.csv")

sofas = pd.read_csv("/data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Prescient"+site+"/raw/"+id+"/surveys/"+id+"_SOFAS.csv")

# All csvs have LastModifiedDate, subjectkey, interview_date, interview_age, gender, visit
# making a list of names for each of the forms
form_names = ['bprs', 'cdss', 'coenrollment', 'demos', 'health', 'inclusion', 'lifetime', 'missing', 'pgis', 'premorbidiq', 'promis', 'recruitment', 'inclusion', 'sofas']

# making a list of the dataframes
forms = [bprs, cdss, coenrollment, demos, health, inclusion, lifetime, missing, pgis, premorbidiq, promis, recruitment, inclusion, sofas]

# creating a dataframe to store all the info
forms_info = pd.DataFrame(index = form_names, columns = ['Visit', 'Interview_date', 'Total_Variables', 'Percent_complete'])

dpdash_tot = pd.DataFrame(index = form_names, columns = ['Value'])
dpdash_percent = pd.DataFrame(index = form_names, columns = ['Value'])

for df,name in zip(forms,form_names):
	#print(df.T)
	#print('Visit: ', df.at[0, 'visit'])
	num_var = len(df.T) - 6
	#print("Number of variables: ", num_var)
	missing = df.isnull().sum(axis=1)[0]
	#print('Missing variables: ', missing)
	percent = 100 - round((missing/num_var)*100)

	forms_info.at[name, 'Visit'] = df.at[0, 'visit']
	forms_info.at[name, 'Interview_date'] = df.at[0, 'interview_date']
	forms_info.at[name, 'Total_Variables'] = num_var
	forms_info.at[name, 'Percent_complete'] = percent

	dpdash_tot.at[name, 'Value'] = num_var
	dpdash_percent.at[name, 'Value'] = percent

print(forms_info)

dpdash_percent.index = dpdash_percent.index.str.replace('(.*)', r'\1_perc') 
dpdash_tot.index = dpdash_tot.index.str.replace('(.*)', r'\1_tot') 
#print(dpdash_percent)
#print(dpdash_tot)

# concatenating all of the measures
frames = [dpdash_percent, dpdash_tot]
dp_con = pd.concat(frames)

# reorganizing measures
dp_con = dp_con.sort_index(axis = 0)
dp_con = dp_con.transpose()
#print(dp_con)

names_dash = ['day', 'reftime', 'timeofday', 'weekday', 'subjectid', 'site', 'mtime']

dpdash_main = pd.DataFrame(columns = names_dash)
dpdash_main.at['Value', 'subjectid'] = id
dpdash_main.at['Value', 'site'] = site
dpdash_main.at['Value', 'mtime'] = inclusion.at[0, 'interview_date']
day = '1'
dpdash_main.at['Value', 'day'] = day

frames = [dpdash_main, dp_con]
dpdash_full = pd.concat(frames, axis=1)
print(dpdash_full.T)


# saving the csv
dpdash_full.to_csv("Prescient_status/formqc-"+id+"-percent-day1to"+day+".csv", sep=',', index = False, header=True)

dpdash_full.to_csv(output_path+"formqc-"+id+"-percent-day1to"+day+".csv", sep=',', index = False, header=True)












