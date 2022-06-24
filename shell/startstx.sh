#!/bin/bash
export CRT_DIR=${PWD}
source ${HOME}/.env_vars
source ${HOME}/.envs/stx/bin/activate
BASE_DIR=${HOME}/stx
cd ${BASE_DIR}/python
export LANG=en_US.UTF-8
export FLASK_APP=stxws
export FLASK_ENV=development
flask run
cd ${CRT_DIR}
