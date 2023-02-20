#!/bin/bash

#import python first with b7

LOGFILE=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient.log
LOGERR=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient_err.log

exec 2> $LOGERR 1> $LOGFILE

#ls /data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | sed 's:.*/::' | cut -d '.' -f1 > pronet_sub_list_chr.txt
#find /data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | sed 's:.*/::' | cut -d '.' -f1 > pronet_sub_list_chr.txt

####### gives date as well for combined
echo "Prescient participant List: "
# most recently updated prescient participants
ls /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/*.csv | sed 's:.*/::' | cut -d '_' -f1 | cut -d '.' -f1 | sort | uniq > prescient_sub_list_recent.txt

# all prescient participants for combined
#ls -l --time-style=+"%Y-%m-%d" /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/*informed_consent_run_sheet.csv | awk '{print $6, $7}' | sed 's:/.*/:/:' | sed "s/\///1" | cut -d '_' -f1 > prescient_sub_list.txt
ls -l --time-style=+"%Y-%m-%d" /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/* | awk '{print $6, $7}' | sed 's:/.*/:/:' | sed "s/\///1" | cut -d '_' -f1 | cut -d '.' -f1 | sort -r  -k1 | sort -k2 -u > prescient_sub_list.txt

cat prescient_sub_list.txt

echo "Total Number of Prescient Participants: "
cat prescient_sub_list.txt | wc -l


#### creating csvs for forms for all prescient participants
echo "Creating csvs - Prescient"
cat prescient_sub_list_recent.txt | while read sub; do

  rm /data/predict1/data_from_nda/formqc/*$sub*day*
  python forms_qc_ind_csv_prescient.py $sub
  echo ""
  echo ""
  echo ""

done 


###### Individual Pronet participants
#find /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json -mtime -6 | sed 's:.*/::' | cut -d '.' -f1 > pronet_sub_list_recent.txt
#ls /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | sed 's:.*/::' | cut -d '.' -f1 > pronet_sub_list_recent.txt

cat pronet_sub_list_recent.txt

echo "Total Number of Pronet Participants: "
cat pronet_sub_list_recent.txt | wc -l

#### creating csvs for forms for all pronet participants
echo "Creating csvs - Pronet"
#cat pronet_sub_list_recent.txt | while read sub; do
  #rm /data/predict/data_from_nda/formqc/*$sub*day*
  #python forms_qc_ind_csv_updated.py $sub
  #echo ""
  #echo ""
  #echo ""

#done

echo "Pronet participant List: "
ls -l --time-style=+"%Y-%m-%d" /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | awk '{print $6, $7}' | sed 's:/.*/:/:' | sed "s/\///1" | cut -d '.' -f1 > pronet_sub_list_chr.txt

#cat pronet_sub_list_chr.txt

echo "Total Number of Pronet Participants: "
cat pronet_sub_list_chr.txt | wc -l

#############################################################################
### combining forms for prescient
echo "Combining forms for prescient participants"
python combined_forms_prescient.py

### combining forms_qc files into a single file for pronet participants
echo "Combining forms for pronet participants"
#python combined_forms_qc.py


#############################################################################
# Uploading data to dpdash
bash /data/predict/utility/dpimport_formqc.sh /data/predict1/data_from_nda/ rc-predict

#bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict

#https://predict.bwh.harvard.edu/dpdash/dashboard/files/ProNET



