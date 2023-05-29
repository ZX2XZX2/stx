#!/bin/bash
export CRT_DIR=${PWD}
BASE_DIR=${HOME}/stx
source ${HOME}/.env_vars
source ${HOME}/.envs/stx/bin/activate
cd ${BASE_DIR}/python
export LANG=en_US.UTF-8
python stxdf.py -i ${HOME}/data5/data_5_1.txt
python stxdf.py -i ${HOME}/data5/data_5_2.txt
python stxdf.py -i ${HOME}/data5/data_5_3.txt
python stxdf.py -i ${HOME}/data5/data_5_4.txt
python stxdf.py -i ${HOME}/data5/data_5_5.txt
python stxdf.py -i ${HOME}/data5/data_5_6.txt
python stxdf.py -i ${HOME}/data5/data_5_7.txt
python stxdf.py -i ${HOME}/data5/data_5_8.txt
python stxdf.py -i ${HOME}/data5/data_5_9.txt
python stxdf.py -i ${HOME}/data5/data_5_10.txt
python stxdf.py -i ${HOME}/data5/data_5_11.txt
python stxdf.py -i ${HOME}/data5/data_5_12.txt
python stxdf.py -i ${HOME}/data5/data_5_13.txt
python stxdf.py -i ${HOME}/data5/data_5_14.txt
python stxdf.py -i ${HOME}/data5/data_5_15.txt
python stxdf.py -i ${HOME}/data5/data_5_16.txt
python stxdf.py -i ${HOME}/data5/data_5_17.txt
python stxdf.py -i ${HOME}/data5/data_5_18.txt
python stxdf.py -i ${HOME}/data5/data_5_19.txt
python stxdf.py -i ${HOME}/data5/data_5_20.txt
python stxdf.py -i ${HOME}/data5/data_5_21.txt
python stxdf.py -i ${HOME}/data5/data_5_22.txt
cd ${CRT_DIR}
