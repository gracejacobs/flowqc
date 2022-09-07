#!/bin/bash

#import python first with b7

echo "Pronet participant List: "
cat pronet_sub_list_chr.txt

echo "Prescient participant List: "
cat prescient_sub_list.txt

#### creating csvs for forms for all prescient participants
echo "Creating csvs - Prescient"
cat prescient_sub_list.txt | while read sub; do
  #rm /data/predict/data_from_nda/formqc/*$sub*
  python forms_qc_ind_csv_prescient.py $sub
done 

### combining forms for prescient
python combined_forms_prescient.py

### combining forms_qc files into a single file including screening
echo "Combining forms for pronet participants"
python combined_forms_qc.py


# Uploading data to dpdash
bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict

#https://predict.bwh.harvard.edu/dpdash/dashboard/files/ProNET



