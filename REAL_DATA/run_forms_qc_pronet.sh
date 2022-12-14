#!/bin/bash

#source /data/pnl/soft/pnlpipe3/bashrc3


echo "Pronet participant List: "

find /data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | sed 's:.*/::' | cut -d '.' -f1 > pronet_sub_list_recent.txt

cat pronet_sub_list_recent.txt

echo "Total Number of Pronet Participants: "
cat pronet_sub_list_recent.txt | wc -l

#### creating csvs for forms for all pronet participants
echo "Creating csvs - Pronet"
cat pronet_sub_list_recent.txt | while read sub; do

  #rm /data/predict/data_from_nda/formqc/*$sub*day*
  python forms_qc_ind_csv_updated.py $sub
  echo ""
  echo ""
  echo ""

done 

# Uploading data to dpdash
#bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict



