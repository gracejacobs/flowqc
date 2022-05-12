### Checking incoming forms for AMP-SCZ based on a participant's json from PHOENIX

#### forms_qc.py
For each event (e.g., screening, baseline) goes through all of the forms and calculates the number of variables in the participant's json, what percentage of the variables were filled out, and the difference between the interview and data entry dates.

#### combining_all_events.py
Takes the individual csvs for each event and appends them into a master csv that can be uploaded to dpdash.
