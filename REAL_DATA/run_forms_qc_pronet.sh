#!/bin/bash

source /data/pnl/soft/pnlpipe3/bashrc3

echo "Pronet participant List: "
cat pronet_sub_list_chr.txt

#### creating csvs for forms for all pronet participants
echo "Creating csvs - Pronet"
cat pronet_sub_list_chr.txt | while read sub; do
  #rm /data/predict/data_from_nda/formqc/*$sub*
  python forms_qc_ind_csv_updated.py $sub
done 

# Uploading data to dpdash
bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict



