#!/usr/bin/env bash
SHELL=/bin/bash

source /home/ubuntu/.bashrc

export PATH="/home/ubuntu/anaconda3/envs/picarro3/bin:$PATH"

export PYTHONPATH=$PYTHONPATH:/home/ubuntu/gitrepos/Elastic

DATE=`date +%Y-%m-%d_%H-%M-%S`

python -W ignore \
    /home/ubuntu/gitrepos/Elastic/src/dashboard/db_io/import_ls_db_logs.py \
    >> /home/ubuntu/Desktop/realtime_update_logs/db_status/db_import_log.log \
    2>> /home/ubuntu/Desktop/realtime_update_logs/db_status/db_import_error.log