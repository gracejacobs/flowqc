#!/bin/bash

#source /data/pnl/soft/pnlpipe3/bashrc3

LOGFILE=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_pronet.log

#LOGERR=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_pronet_err.log

#LOGOUT=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_forms_qc_pronet_out.log


echo "Pronet participant List: "
ls /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*YA*Pronet.json | sed 's:.*/::' | cut -d '.' -f1 > pronet_sub_list_test.txt
#find /data/predict/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json -mtime -7 | sed 's:.*/::' | cut -d '.' -f1 > pronet_sub_list_recent.txt

cat pronet_sub_list_test.txt

echo "Total Number of Pronet Participants: "
cat pronet_sub_list_test.txt | wc -l

#### creating csvs for forms for all pronet participants
echo "Creating csvs - Pronet"
	
count = 0
cat pronet_sub_list_test.txt | while read sub; do
	count = $((count + 1))
	echo $count
	rm /data/predict1/data_from_nda/formqc/*$sub*day*
	python forms_qc_ind_csv_updated.py $sub
	echo ""
	echo ""
	echo ""

done 


#) >& $LOGFILE



#bash /data/predict/utility/dpimport_formqc.sh /data/predict1/data_from_nda/ rc-predict

# Uploading data to dpdash
#bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda/ rc-predict



