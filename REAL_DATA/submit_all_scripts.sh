#!/bin/bash




bsub -q interactive -Is /bin/bash run_forms_qc_pronet.sh

bsub -q interactive -Is /bin/bash run_forms_qc_all.sh
