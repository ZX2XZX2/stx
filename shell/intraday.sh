#!/bin/bash
export CRT_DIR=${PWD}
source ${HOME}/.env_vars
BASE_DIR=${HOME}/stx
cd ${BASE_DIR}/c
./stx_ana.exe --intraday
cd ${BASE_DIR}/python
source ${HOME}/.envs/stx/bin/activate
export LANG=en_US.UTF-8
python3 stx247.py -i 
cd ${CRT_DIR}
