#!/bin/bash

#import python first with b7


echo "Prescient participant List: "
# most recently updated prescient participants
ls /data/predict-1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/*.csv | sed 's:.*/::' | cut -d '_' -f1 | cut -d '.' -f1 | sort | uniq > prescient_sub_list_recent.txt

cat prescient_sub_list_recent.txt

echo "Total Number of Prescient Participants: "
cat prescient_sub_list_recent.txt | wc -l


#### creating csvs for forms for all prescient participants
echo "Creating csvs - Prescient"
cat prescient_sub_list_recent.txt | while read sub; do

  rm /data/predict1/data_from_nda/formqc/*$sub*day*
  python forms_qc_ind_csv_prescient.py $sub
  echo ""
  echo ""
  echo ""

done 



#############################################################################


#############################################################################
# Uploading data to dpdash
#bash /data/predict-1/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict

#https://predict.bwh.harvard.edu/dpdash/dashboard/files/ProNET



