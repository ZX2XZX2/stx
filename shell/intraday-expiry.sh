#!/bin/bash
export CRT_DIR=${PWD}
source ${HOME}/.env_vars
BASE_DIR=${HOME}/stx
cd ${BASE_DIR}/c
./stx_ana.exe --intraday-expiry
cd ${CRT_DIR}
