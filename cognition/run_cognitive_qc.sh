#!/bin/bash


LOGFILE=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_cognition_qc.log
LOGERR=/data/pnl/home/gj936/U24/Clinical_qc/flowqc/REAL_DATA/logs/run_cognition_qc_err.log

# generating list of participants with cognition jsons

ls /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*UPENN.json | sed 's:.*/::' | cut -d '.' -f1 > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/pronet_cognitive_data.txt

ls /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*UPENN.json | sed 's:.*/::' | cut -d '.' -f1 > /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/prescient_cognitive_data.txt

# generating list of participants with form data indicating 0-2 on penn cnb availability

#python cognitive_form_data.py

######### PRONET DATA

cd /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/

echo "Number of PRONET participants with baseline cognitive data based on forms:"
cat /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/Pronet_BL_cognitive_data_form_available.csv | wc -l

echo "Number of PRONET participants with cognitive data jsons:"
cat /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/pronet_cognitive_data.txt | wc -l

echo ""
echo "PRONET Participants missing jsons although data is indicated to be available in baseline penn form: "
comm -13 <(sort pronet_cognitive_data.txt) <(sort Pronet_BL_cognitive_data_form_available.csv) | wc -l
comm -13 <(sort pronet_cognitive_data.txt) <(sort Pronet_BL_cognitive_data_form_available.csv)

echo "PRONET participants with jsons but did not indicate data available in baseline penn forms (could just have month 2 data):"
comm -13 <(sort Pronet_BL_cognitive_data_form_available.csv) <(sort pronet_cognitive_data.txt) | wc -l
comm -13 <(sort Pronet_BL_cognitive_data_form_available.csv) <(sort pronet_cognitive_data.txt)
#grep -Fxf Pronet_BL_cognitive_data_form_available.csv pronet_cognitive_data.txt | wc -l

########## PRESCIENT DATA
echo ""
echo ""
echo "Number of PRESCIENT  participants with baseline cognitive data based on forms:"
cat /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/Prescient_BL_cognitive_data_form_available.csv | wc -l

echo "Number of PRESCIENT participants with cognitive data jsons:"
cat /data/pnl/home/gj936/U24/Clinical_qc/flowqc/cognition/prescient_cognitive_data.txt | wc -l

echo ""
echo "PRESCIENT Participants missing jsons although data is indicated to be available in baseline penn form: "
comm -13 <(sort prescient_cognitive_data.txt) <(sort Prescient_BL_cognitive_data_form_available.csv) | wc -l
comm -13 <(sort prescient_cognitive_data.txt) <(sort Prescient_BL_cognitive_data_form_available.csv)

echo "PRESCIENT Participants with jsons but did not indicate data available in baseline penn forms:"
comm -13 <(sort Prescient_BL_cognitive_data_form_available.csv) <(sort prescient_cognitive_data.txt) | wc -l
comm -13 <(sort Prescient_BL_cognitive_data_form_available.csv) <(sort prescient_cognitive_data.txt)
#grep -Fxf Pronet_BL_cognitive_data_form_available.csv pronet_cognitive_data.txt | wc -l











