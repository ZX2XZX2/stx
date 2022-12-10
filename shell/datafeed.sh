#!/bin/bash                                                                     
export CRT_DIR=${PWD}
BASE_DIR=${HOME}/stx
source ${HOME}/.env_vars
source ${HOME}/.envs/stx/bin/activate
cd ${BASE_DIR}/python
export LANG=en_US.UTF-8
python3 stxdf.py
python3 stxdivi.py
cd ${CRT_DIR}
