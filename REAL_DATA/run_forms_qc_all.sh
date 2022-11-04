#!/bin/bash

#import python first with b7

echo "Pronet participant List: "
ls /data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | sed 's:.*/::' | cut -d '.' -f1 > pronet_sub_list_chr.txt
cat pronet_sub_list_chr.txt
echo "Total Number of Pronet Participants: "
cat pronet_sub_list_chr.txt | wc -l

echo "Prescient participant List: "
ls /data/predict/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/*informed_consent_run_sheet.csv | sed 's:.*/::' | cut -d '_' -f1 > prescient_sub_list.txt
cat prescient_sub_list.txt
echo "Total Number of Prescient Participants: "
cat prescient_sub_list.txt | wc -l



#### creating csvs for forms for all prescient participants
echo "Creating csvs - Prescient"
cat prescient_sub_list.txt | while read sub; do
  rm /data/predict/data_from_nda/formqc/*$sub*day*
  python forms_qc_ind_csv_prescient.py $sub
done 



### combining forms for prescient
python combined_forms_prescient.py

### combining forms_qc files into a single file for pronet participants
echo "Combining forms for pronet participants"
python combined_forms_qc.py


# Uploading data to dpdash
bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict

#https://predict.bwh.harvard.edu/dpdash/dashboard/files/ProNET



