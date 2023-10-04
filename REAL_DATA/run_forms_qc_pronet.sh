#!/bin/bash

source /data/pnl/soft/pnlpipe3/bashrc3

cd /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/

# creating log files
echo $(date +"%d-%m-%Y") > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_pronet_$(date +"%d-%m-%Y").log

echo $(date +"%d-%m-%Y") > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_pronet_err_$(date +"%d-%m-%Y").log
chmod 777 /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_pronet_err_$(date +"%d-%m-%Y").log

LOGFILE=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_pronet_$(date +"%d-%m-%Y").log

LOGERR=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_pronet_err_$(date +"%d-%m-%Y").log

echo ${LOGFILE}
echo ${LOGERR}

exec 2> $LOGERR 1> $LOGFILE


echo "Pronet participant List: "
# all participants with time
ls -l --time-style=+"%Y-%m-%d" /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | awk '{print $6, $7}' | sed 's:/.*/:/:' | sed "s/\///1" | cut -d '.' -f1 > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/pronet_sub_list_chr.txt

echo "Total Number of Pronet Participants: "
cat /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/pronet_sub_list_chr.txt | wc -l

echo "Pronet participant List: "
# all participants - no time
ls /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | sed 's:.*/::' | cut -d '.' -f1 > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/pronet_sub_list.txt
## most recent participants - no time
find /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json -mtime -2 | sed 's:.*/::' | cut -d '.' -f1 > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/pronet_sub_list_recent.txt


cat /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/pronet_sub_list_recent.txt

#############################################################################################
#python combined_forms_qc.py

#### creating csvs for forms for all pronet participants
echo "Creating csvs - Pronet"
	
cat pronet_sub_list_recent.txt | while read sub; do
	rm /data/predict1/data_from_nda/formqc/*$sub*day*

	python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/forms_qc_ind_both_networks.py $sub PRONET formqc
	echo ""
	echo ""
	echo ""

done 




#############################################################################################

echo "Creating combined csvs: "

python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/combined_forms_both_networks.py PRONET formqc


python /data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/creating_summary_csv.py

echo "Pronet script complete"

#############################################################################################
# importing data

#bash /data/predict1/utility/dpimport_formqc.sh /data/predict1/data_from_nda/ rc-predict

# Uploading data to dpdash
#bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict



