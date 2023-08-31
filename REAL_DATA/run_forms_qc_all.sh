#!/bin/bash

#import python first with b7
source /data/pnl/soft/pnlpipe3/bashrc3

cd /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/

#LOGFILE=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient.log
#LOGERR=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient_err.log


# creating files
echo $(date +"%d-%m-%Y") > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient_$(date +"%d-%m-%Y").log

echo $(date +"%d-%m-%Y") > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient_err_$(date +"%d-%m-%Y").log
chmod 777 /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient_err_$(date +"%d-%m-%Y").log

LOGFILE=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient_$(date +"%d-%m-%Y").log

LOGERR=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_prescient_err_$(date +"%d-%m-%Y").log

echo ${LOGFILE}
echo ${LOGERR}



exec 2> $LOGERR 1> $LOGFILE


# pronet subject list for cognitive data - everyone
ls /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | sed 's:.*/::' | cut -d '.' -f1 > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/pronet_sub_list.txt

echo "Prescient participant List: "
# most recently updated prescient participants - raw csvs
ls /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/*[[:upper:]]*.csv | sed 's:.*/::' | cut -d '_' -f1 | cut -d '.' -f1 | sort | uniq > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/prescient_sub_list_recent.txt

# json list
ls -l --time-style=+"%Y-%m-%d" /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Prescient.json | awk '{print $6, $7}' | sed 's:/.*/:/:' | sed "s/\///1" | cut -d '.' -f1 > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/prescient_json_sub_list_combined.txt
# all prescient participants for combined - raw csvs
ls -l --time-style=+"%Y-%m-%d" /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/*[[:upper:]]* | awk '{print $6, $7}' | sed 's:/.*/:/:' | sed "s/\///1" | cut -d '_' -f1 | cut -d '.' -f1 | sort -r  -k1 | sort -k2 -u > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/prescient_sub_list.txt


########################################################################################
cat /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/prescient_sub_list.txt

echo "Total Number of Prescient Participants: "
cat /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/prescient_sub_list.txt | wc -l

########################################################################################
########################################################################################
#echo "Combining forms for prescient participants"
#python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/combined_forms_prescient.py

########################################################################################
#### creating csvs for forms for all prescient participants
echo "Creating csvs - Prescient"
cat prescient_sub_list_recent.txt | while read sub; do
  echo "Generating individual forms"
  rm /data/predict1/data_from_nda/formqc/*$sub*day*
  python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/forms_qc_ind_csv_prescient.py $sub
  #rm /data/predict1/data_from_nda/formqc_test/*$sub*day*
  python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/forms_qc_ind_both_networks.py $sub PRESCIENT formqc_test
  echo ""
  echo ""
  echo ""

done 

########################################################################################

### combining forms for prescient
echo "Combining forms for prescient participants"
python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/combined_forms_prescient.py

########################################################################################
### generating cognition summaries for participants
echo "Creating cognitive summaries for participants"
rm /data/predict1/data_from_nda/formqc/*cognition_summary*

python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/combining_cognitive_data.py PRESCIENT

python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/combining_cognitive_data.py PRONET

#############################################################################
### combining forms for prescient
echo "Combining forms for prescient participants using jsons"
python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/combined_forms_both_networks.py PRESCIENT formqc_test


echo "Prescient script complete"


#############################################################################
# Uploading data to dpdash
#bash /data/predict1/utility/dpimport_formqc.sh /data/predict1/data_from_nda/ rc-predict

#bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict

#https://predict.bwh.harvard.edu/dpdash/dashboard/files/ProNET



