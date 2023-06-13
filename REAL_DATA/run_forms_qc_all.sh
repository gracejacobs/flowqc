#!/bin/bash

#import python first with b7

LOGFILE=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient.log
LOGERR=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient_err.log

exec 2> $LOGERR 1> $LOGFILE


# pronet subject list for cognitive data - everyone
ls /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | sed 's:.*/::' | cut -d '.' -f1 > pronet_sub_list.txt

echo "Prescient participant List: "
# most recently updated prescient participants - raw csvs
ls /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/*[[:upper:]]*.csv | sed 's:.*/::' | cut -d '_' -f1 | cut -d '.' -f1 | sort | uniq > prescient_sub_list_recent.txt

# json list
ls -l --time-style=+"%Y-%m-%d" /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Prescient.json | awk '{print $6, $7}' | sed 's:/.*/:/:' | sed "s/\///1" | cut -d '.' -f1 > prescient_json_sub_list_combined.txt
# all prescient participants for combined - raw csvs
ls -l --time-style=+"%Y-%m-%d" /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/*[[:upper:]]* | awk '{print $6, $7}' | sed 's:/.*/:/:' | sed "s/\///1" | cut -d '_' -f1 | cut -d '.' -f1 | sort -r  -k1 | sort -k2 -u > prescient_sub_list.txt


########################################################################################
cat prescient_sub_list.txt

echo "Total Number of Prescient Participants: "
cat prescient_sub_list.txt | wc -l

########################################################################################
########################################################################################
echo "Combining forms for prescient participants"
python combined_forms_prescient.py

########################################################################################
#### creating csvs for forms for all prescient participants
echo "Creating csvs - Prescient"
cat prescient_sub_list_recent.txt | while read sub; do
  echo "Generating individual forms"
  #rm /data/predict1/data_from_nda/formqc/*$sub*day*
  #python forms_qc_ind_csv_prescient.py $sub
  rm /data/predict1/data_from_nda/formqc_test/*$sub*day*
  python forms_qc_ind_both_networks.py $sub PRESCIENT
  echo ""
  echo ""
  echo ""

done 

########################################################################################

### combining forms for prescient
echo "Combining forms for prescient participants"
python combined_forms_prescient.py

########################################################################################
### generating cognition summaries for participants
echo "Creating cognitive summaries for participants"
rm /data/predict1/data_from_nda/formqc/*cognition_summary*

python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/combining_cognitive_data.py PRESCIENT

python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/combining_cognitive_data.py PRONET


#############################################################################
### combining forms for prescient
#echo "Combining forms for prescient participants"
#python combined_forms_prescient.py


#############################################################################
# Uploading data to dpdash
#bash /data/predict1/utility/dpimport_formqc.sh /data/predict1/data_from_nda/ rc-predict

#bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict

#https://predict.bwh.harvard.edu/dpdash/dashboard/files/ProNET



