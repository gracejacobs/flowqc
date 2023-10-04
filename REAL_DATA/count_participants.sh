#!/bin/bash

echo "Total Number of ProNET Participants: "
ls /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | sed 's:.*/::' | cut -d '.' -f1 | wc -l

#ls /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pronet.json | sed 's:.*/::' | cut -d '.' -f1

echo "Total Number of PRESCIENT Participants: "
ls /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/*[[:upper:]]*.csv | sed 's:.*/::' | cut -d '_' -f1 | cut -d '.' -f1 | sort | uniq | wc -l

echo "Total Number of PRESCIENT Participants with jsons: "
ls /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Prescient.json | sed 's:.*/::' | cut -d '.' -f1 | wc -l

echo "ALL Participants with jsons: "
ls /data/predict1/data_from_nda/Pr*/PHOENIX/PROTECTED/Pr*/raw/*/surveys/*Pr*.json | sed 's:.*/::' | cut -d '.' -f1 | wc -l

#ls /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/Pres*/raw/*/surv*/*[[:upper:]]*.csv | sed 's:.*/::' | cut -d '_' -f1 | cut -d '.' -f1 | sort | uniq

