### Checking incoming forms for AMP-SCZ based on a participant's json from PHOENIX

All updated code is in REAL_DATA.

There are two master scripts to set up and import data to dpdash:

1) run_forms_qc_pronet.sh
2) run_forms_qc_all.sh

#### forms_qc.py
For each event (e.g., screening, baseline) goes through all of the forms and calculates the number of variables in the participant's json, what percentage of the variables were filled out, and the difference between the interview and data entry dates.

#### combining_all_events.py
Takes the individual csvs for each event and appends them into a master csv that can be uploaded to dpdash.

#### getting the config template
cd /data/predict/utility
source .vault/.env.dpstage OR
source .vault/.env.rc-predict

./download_config.sh screening-forms /data/pnl/home/gj936/U24/Clinical_qc/flowqc/configs/screening-forms.json

#### making_dpdash_config.py
Takes the formqc-template.json template and applies it to all of the variables based on the Column_names.csv generated in combining_all_events.py

The new config file created from this script needs to be manually uploaded to dpdash and tagged as the default before running **/data/predict/utility/dpimport_formqc.sh** to import the csvs located in /data/predict/kcho/flow_test/formqc/

#### gen_data_from_forms.py
Creates a csv of available data for each instrument
