#!/bin/bash
export CRT_DIR=${PWD}
source ${HOME}/.env_vars
source ${HOME}/.envs/stx/bin/activate
BASE_DIR=${HOME}/stx
cd ${BASE_DIR}/c
./stx_ana.exe --eod
cd ${BASE_DIR}/python
export LANG=en_US.UTF-8
python3 stx247.py -e -s 15
cd ${CRT_DIR}
