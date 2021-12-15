#!/bin/bash

echo -e "This script assumes that linux_setup.sh has already run"

echo -e "\n\n1. Remember current directory to return after script execution"
CRT_DIR=$(dirname $0)
cd "${CRT_DIR}" && CRT_DIR=$PWD

# Print script usage
USAGE="\nUsage: $0 [--usb-drive /path/to/usb_drive] [--jdk-file /path/to/jdk-file] [--db-bkp-file /path/to/db-bkp-file]\n"

echo -e "\n\n2. Parse the input arguments"
ops='usb-drive:,jdk-file:,db-bkp-file:'
declare {USB_DRIVE,JDK_FILE,DB_BKP_FILE}=''
OPTIONS=$(getopt --options '' --longoptions ${ops} --name "$0" -- "$@")
[[ $? != 0 ]] && exit 3

eval set -- "${OPTIONS}"

while true
do
    case "${1}" in
	--usb-drive)
	    USB_DRIVE="$2"
	    shift 2
	    ;;
	--jdk-file)
	    JDK_FILE="$2"
	    shift 2
	    ;;
	--db-bkp-file)
	    DB_BKP_FILE="$2"
	    shift 2
	    ;;
	--)
	    shift
	    break
	    ;;
	*)
	    echo -e "\n\nUndefined options given!"
	    echo "$*"
	    echo -e "${USAGE}"
	    exit 3
	    ;;
    esac
done

echo -e "\n\n3. Check that USB drive location, the JDK file and the DB backup location were specified"
[[ "${USB_DRIVE}" == '' ]] && (echo -e "\nError: no USB drive location specified! Check script usage.\n${USAGE}" && exit 1)
[[ "${JDK_FILE}" == '' ]] && (echo -e "\nError: no JDK file location specified! Check script usage.\n${USAGE}" && exit 1)
[[ "${DB_BKP_FILE}" == '' ]] && (echo -e "\nError: no DB backup file specified! Check script usage.\n${USAGE}" && exit 1)

echo -e "Running script with the following parameters:"
echo -e "  USB Drive: ${USB_DRIVE}"
echo -e "  JDK File: ${JDK_FILE}"
echo -e "  DB backup files: ${DB_BKP_FILE}"

echo -e "\n\n4. Initialize environment variables and directories"
export ENV_VARS_FILE=${HOME}/.env_vars
source ${ENV_VARS_FILE}
export GIT_DIR=${HOME}/stx
export C_DIR=${GIT_DIR}/c
export JAVA_DIR=${GIT_DIR}/java
export PYTHON_DIR=${GIT_DIR}/python
export SHELL_DIR=${GIT_DIR}/shell

echo -e "\n\n5. Install python packages in stx virtual environment"
cd ${PYTHON_DIR}
source $HOME/.envs/stx/bin/activate
pip3 install -r requirements.txt

echo -e "\n\n6. Install/configure Java stuff"
JDK_INSTALL_DIR=/usr/lib/jvm
sudo mkdir -p ${JDK_INSTALL_DIR}
sudo cp ${USB_DRIVE}/${JDK_FILE} ${JDK_INSTALL_DIR}
sudo cd ${JDK_INSTALL_DIR}
sudo tar zxf ${JDK_FILE}
echo "export PATH=${JDK_INSTALL_DIR}/jdk1.8.0_291/bin:"'${PATH}' >> ${HOME}/.bashrc
mkdir -p ${JAVA_DIR}/output
cd ${JAVA_DIR}/output
curl -O https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar

echo -e "\n\n7. Populate the Database"
cd ${SHELL_DIR}
export DB_BACKUP_DIR=${DB_BKP_FILE}
./restore_db.sh stx stx

echo -e "\n\n8. Setup cron mailer"
sudo apt install -y postfix mailutils

echo -e "\n\n 9. Generate stx_cfg.ini file"
echo "[datafeed]" >> ${HOME}/stx_cfg.ini
echo "data_dir = ${DATA_DIR}" >> ${HOME}/stx_cfg.ini
echo "download_dir = ${DOWNLOAD_DIR}" >> ${HOME}/stx_cfg.ini
echo "[postgres_db]" >> ${HOME}/stx_cfg.ini
echo "db_name = ${POSTGRES_DB}" >> ${HOME}/stx_cfg.ini
echo "usb_1 = ${USB_1}" >> ${HOME}/stx_cfg.ini
echo "usb_2 = ${USB_2}" >> ${HOME}/stx_cfg.ini
echo "usb_3 = ${USB_3}" >> ${HOME}/stx_cfg.ini

cd ${CRT_DIR}
