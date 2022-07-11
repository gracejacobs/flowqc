#!/usr/bin/bash


cd /data/predict/utility
source .vault/.env.dpstage

# downloading skeletal config
./download_config.sh formqc-1 /tmp/formqc-1.json

# get column names from csv


# create a json for dpdash based on config and column names
python make_dpdash_config.py
