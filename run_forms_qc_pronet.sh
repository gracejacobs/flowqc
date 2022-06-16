#!/bin/bash

#import python first with b7

echo "Participant List: "
cat pronet_mock_list.txt


# getting data for all participants

#cat pronet_mock_list.txt | while read sub; do
#  python forms_qc.py $sub
#done 



# Combining screening and baseline
cat pronet_mock_list.txt | while read sub; do
  python combining_all_events.py $sub
done 


source /data/predict/utility/.vault/.env.dpstage
bash /data/predict/utility/dpimport_formqc.sh


#cat formqc-LA00028-percent-day1to114.csv | while read sub; do; echo $sub; done
