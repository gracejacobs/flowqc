#!/bin/bash

#import python first with b7

echo "Pronet participant List: "
cat pronet_sub_list.txt

echo "Prescient participant List: "
cat prescient_sub_list.txt


#### getting data for all pronet participants
echo "Creating csvs - Pronet"
cat pronet_sub_list.txt | while read sub; do
  python forms_qc.py $sub
done 

#### Combining screening and baseline for pronet
echo "Combining csvs - Pronet"
cat pronet_sub_list.txt | while read sub; do
  python combining_all_events.py $sub
done 

### mri run sheet
#print("Creating mri_run_sheet csvs - Pronet")
#cat pronet_sub_list.txt | while read sub; do
#  python assessments/pronet_mri_formsqc.py $sub
#done 

#### getting csvs for prescient participants
echo "Creating csvs - Prescient"
cat prescient_sub_list.txt | while read sub; do
  python forms_qc_prescient.py $sub
done 

### combining forms_qc files into a single file including baseline and screening
echo "Combining forms_qc screening and baseline data across participants"
python forms_qc_combiner.py


# Uploading data to dpdash
bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict

#https://predict.bwh.harvard.edu/dpdash/dashboard/files/ProNET


#cat formqc-LA00028-percent-day1to114.csv | while read sub; do; echo $sub; done
