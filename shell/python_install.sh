#!/bin/bash

# Installs Python 3.12 and the stx virtual environment with all dependencies.
# Can be run independently or re-run safely (idempotent).

CRT_DIR=$(dirname $0)
cd "${CRT_DIR}" && CRT_DIR=$PWD

PYTHON_DIR=${HOME}/stx/python

echo -e "\n1. Add deadsnakes PPA and install Python 3.12\n"
sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt update
sudo apt install -y python3.12 python3.12-venv python3.12-dev

echo -e "\n2. Create stx virtual environment (Python 3.12)\n"
python3.12 -m venv ${HOME}/.envs/stx

grep -q 'alias workon_stx' ${HOME}/.bashrc || \
    echo "alias workon_stx='source \${HOME}/.envs/stx/bin/activate'" >> ${HOME}/.bashrc

echo -e "\n3. Install Python packages\n"
source ${HOME}/.envs/stx/bin/activate
pip install --upgrade pip
pip install -r ${PYTHON_DIR}/requirements.txt

cd ${CRT_DIR}
echo -e "\nDone. Activate the environment with: source ~/.envs/stx/bin/activate\n"
