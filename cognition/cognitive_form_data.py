import sys
import numpy as np
import pandas as pd
import argparse
import json
import os
from datetime import datetime
from datetime import date

input = "/data/predict1/data_from_nda/formqc/"
output = "/data/predict1/data_from_nda/formqc/"
output2 = '/data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/'

## getting today's date
today = date.today()
today = today.strftime("%Y-%m-%d")

#print("Reading in combined csvs")
screening_pronet = pd.read_csv(input + "combined-PRONET-form_screening-day1to1.csv", low_memory=False)
baseline_pronet = pd.read_csv(input + "combined-PRONET-form_baseline-day1to1.csv", low_memory=False)
screening_prescient = pd.read_csv(input + "combined-PRESCIENT-form_screening-day1to1.csv", low_memory=False)
baseline_prescient = pd.read_csv(input + "combined-PRESCIENT-form_baseline-day1to1.csv", low_memory=False)


#print("Subsetting columns of screening")
screening_pronet_1 = screening_pronet[['subjectid', 'site', 'chrguid_guid', 'chric_consent_date', 'chrcrit_part', 'chrcrit_included', 'visit_status_string']]
#screening_pronet_1['Network'] = "Pronet"

screening_prescient_1 = screening_prescient[['subjectid', 'site', 'chrguid_guid', 'chric_consent_date', 'chrcrit_part', 'chrcrit_included', 'visit_status_string']]
#screening_prescient_1['Network'] = "Prescient"

#baseline_pronet_1 = baseline_pronet[['subjectid', 'interview_age', 'chrdemo_sexassigned','chrdemo_education', 'chrdemo_parent_edu_first', 'chrdemo_parent_edu_other' , 'chrdemo_hispanic_latino', 'chrdemo_racial_back___1', 'chrdemo_racial_back___2', 'chrdemo_racial_back___3', 'chrdemo_racial_back___4', 'chrdemo_racial_back___5', 'chrdemo_racial_back___6', 'chrdemo_racial_back___7', 'chrdemo_racial_back___8']]

baseline_pronet_cog = baseline_pronet[['subjectid', 'chrpenn_missing', "chrpenn_interview_date", "chrpenn_entry_date", "chrpenn_complete",  "penncnb_complete"]]

baseline_prescient_cog = baseline_prescient[['subjectid', 'chrpenn_missing', "chrpenn_interview_date", "chrpenn_complete"]]

#print("Merging dataframes")
#print(screening_pronet_1)
#print(baseline_pronet_1)
#print(baseline_pronet_cog)

pronet_all = pd.merge(screening_pronet_1,  baseline_pronet_cog, on=['subjectid'])
prescient_all = pd.merge(screening_prescient_1,  baseline_prescient_cog, on=['subjectid'])

## Subsetting to get list with cognitive data
#print("Printing chrpenn_complete column: ")
#print(pronet_all['chrpenn_complete'].to_string(index=False))
#df.loc[df['column_name'] == ]
pronet_cog = pronet_all
prescient_cog = prescient_all
#print(pronet_cog.dtypes)

pronet_cog = pronet_cog[(pronet_cog['chrpenn_complete'] > -1) & (pronet_cog['chrpenn_complete'] < 3)]
prescient_cog = prescient_cog[(prescient_cog['chrpenn_complete'] > -1) & (prescient_cog['chrpenn_complete'] < 3)]

#print("Printing subset")
#print(pronet_cog['chrpenn_complete'].to_string(index=False))

pronet_cog = pronet_cog[['subjectid']]
prescient_cog = prescient_cog[['subjectid']]

#print(prescient_cog)

### Saving all cognitive data output
pronet_all.to_csv(output + "Pronet_BL_cognitive_data.csv", sep=',', index = False, header=True)

pronet_cog.to_csv(output2 + "Pronet_BL_cognitive_data_form_available.csv", sep=',', index = False, header=False)


prescient_all.to_csv(output + "Prescient_BL_cognitive_data.csv", sep=',', index = False, header=True)

prescient_cog.to_csv(output2 + "Prescient_BL_cognitive_data_form_available.csv", sep=',', index = False, header=False)






