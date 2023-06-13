## Penn Cognitive Data in AMP SCZ

### cognitive_form_data.py

1. Pulls screening and baseline combined csvs from /data/predict1/data_from_nda/formqc/
2. Generates list of participants who should have penn cnb data based on the penn cnb form at baseline (Pronet_BL_cognitive_data_form_available.csv and Prescient_BL_cognitive_data_form_available.csv)

### run_cognitive_qc.sh

1. Generates a list of PRESCIENT and ProNET participants with penn cnb jsons (pronet_cognitive_data.txt and prescient_cognitive_data.txt)
2. Compares lists with the number of participants who should have penn cnb data based on the penn cnb form at baseline (Pronet_BL_cognitive_data_form_available.csv and Prescient_BL_cognitive_data_form_available.csv)


### combining_cognitive_data.py

1. For every ProNET and PRESCIENT participants, it reads in the penn cnb json if it is available
2. Divides up the sessions by timepoint - this is done by ordering them based on date and checking the ending of the session_battery name
3. Creates variables for whether each timepoint has cognitive data available for each participant
4. Creates a cognition status variable for the charts
5. Creates a variable for each cognitive test's valid code. Each row for this variable is a different session timepoint.
6. Outputs a csv with these variables to /data/predict1/data_from_nda/formqc/
7. If a participant does not have any cognitive data, a csv will still be generated but the variables will be blank (for dpdash chart purposes) 


