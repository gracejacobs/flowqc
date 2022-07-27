#!/bin/bash

#import python first with b7

echo "PRONET MOCK LIST: "
cat pronet_mock_list.txt

echo "PRESCIENT MOCK LIST: "
cat prescient_mock_list.txt


#### getting data for all pronet participants
#echo "CREATING CSVS - Pronet"
#cat pronet_mock_list.txt | while read sub; do
#  python forms_qc.py $sub
#done 

#### Combining screening and baseline for pronet
#echo "COMBINING CSVS - Pronet"
#cat pronet_mock_list.txt | while read sub; do
#  python combining_all_events.py $sub
#done 

### mri run sheet
#echo "Creating mri_run_sheet csvs - Pronet"
#cat pronet_mock_list.txt | while read sub; do
#  python assessments/pronet_mri_formsqc.py $sub
#done 

#### getting csvs for prescient participants
#echo "CREATING CSVS - Prescient"
#cat prescient_mock_list.txt | while read sub; do
#  python forms_qc_prescient.py $sub
#done 

### combining forms_qc into a single screening file
#echo "COMBINING FORMS_QC SCREENING AND BASELINE"
#python forms_qc_combiner.py

### indiviudal csvs for pronet
echo "Creating individual csvs - Pronet"
cat pronet_mock_list.txt | while read sub; do
  python forms_qc_ind_csv.py $sub
done 


# Uploading data to dpdash
#source /data/predict/utility/.vault/.env.dpstage
bash /data/predict/utility/dpimport_formqc.sh /data/predict/data_from_nda_dev/ dpstage


#cat formqc-LA00028-percent-day1to114.csv | while read sub; do; echo $sub; done
