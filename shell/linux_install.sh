#!/bin/bash

# Full installation script for the stx project (Ubuntu 22.04/24.04).
# Merges linux_install.sh and linux_setup_2.sh into a single script.

CRT_DIR=$(dirname $0)
cd "${CRT_DIR}" && CRT_DIR=$PWD

USAGE="\nUsage: $0 --env-vars-file /path/to/env_vars_file [--postgres-version 16] [--usb-drive /path/to/usb_drive] --db-bkp-file /path/to/db-bkp-file\n"

ops='env-vars-file:,postgres-version:,usb-drive:,db-bkp-file:'
declare {ENV_VARS_FILE,POSTGRES_VERSION,USB_DRIVE,DB_BKP_FILE}=''
OPTIONS=$(getopt --options '' --longoptions ${ops} --name "$0" -- "$@")
[[ $? != 0 ]] && exit 3

eval set -- "${OPTIONS}"

while true; do
    case "${1}" in
        --env-vars-file)
            ENV_VARS_FILE="$2"
            shift 2
            ;;
        --postgres-version)
            POSTGRES_VERSION="$2"
            shift 2
            ;;
        --usb-drive)
            USB_DRIVE="$2"
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

echo -e "1. Check required parameters\n"
[[ "${ENV_VARS_FILE}" == '' ]] && echo -e "\nError: no environment variables file specified!\n${USAGE}" && exit 1
[[ "${DB_BKP_FILE}"   == '' ]] && echo -e "\nError: no DB backup file specified!\n${USAGE}" && exit 1
[[ "${POSTGRES_VERSION}" == '' ]] && POSTGRES_VERSION=18

echo -e "Running with parameters:"
echo -e "  Environment variables file: ${ENV_VARS_FILE}"
echo -e "  Postgres version:           ${POSTGRES_VERSION}"
[[ "${USB_DRIVE}" != '' ]] && echo -e "  USB Drive:                  ${USB_DRIVE}"
echo -e "  DB backup file:             ${DB_BKP_FILE}"

echo -e "\n2. Read environment variables\n"
source ${ENV_VARS_FILE}

export GIT_DIR=${HOME}/stx
export C_DIR=${GIT_DIR}/c
export JAVA_DIR=${GIT_DIR}/java
export PYTHON_DIR=${GIT_DIR}/python
export SHELL_DIR=${GIT_DIR}/shell

echo -e "\n3. Move SSH keys into ~/.ssh\n"
mkdir -p ${HOME}/.ssh
[[ -f ${HOME}/id_ed25519 ]]     && mv ${HOME}/id_ed25519     ${HOME}/.ssh/ && chmod 600 ${HOME}/.ssh/id_ed25519
[[ -f ${HOME}/id_ed25519.pub ]] && mv ${HOME}/id_ed25519.pub ${HOME}/.ssh/

echo -e "\n4. Install base build tools\n"
sudo apt update
sudo apt install -y apt-transport-https ca-certificates curl gnupg software-properties-common \
    lsb-release wget vim bash-completion
sudo apt install -y git build-essential emacs \
    libssl-dev libffi-dev libpq-dev libcurl4-openssl-dev cmake postfix mailutils
sudo apt -y autoremove

echo -e "\n5. Configure git global identity\n"
git config --global user.name  "${GIT_USERNAME}"
git config --global user.email "${GIT_EMAIL}"

echo -e "\n6. Install PostgreSQL ${POSTGRES_VERSION}\n"
sudo install -d -m 0755 /etc/apt/keyrings
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    | sudo gpg --dearmor -o /etc/apt/keyrings/postgresql.gpg
RELEASE=$(lsb_release -cs)
echo "deb [signed-by=/etc/apt/keyrings/postgresql.gpg] https://apt.postgresql.org/pub/repos/apt ${RELEASE}-pgdg main" \
    | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt update
sudo apt install -y postgresql-${POSTGRES_VERSION} postgresql-client-${POSTGRES_VERSION}
systemctl status postgresql.service --no-pager || true

echo -e "\n7. Create PostgreSQL users and database\n"
sudo -u postgres psql -d postgres -c "ALTER USER postgres PASSWORD '${POSTGRES_PASSWORD}'"
sudo -u postgres psql -d postgres -c "CREATE ROLE ${USER} WITH LOGIN SUPERUSER PASSWORD '${POSTGRES_PASSWORD}'" || true
sudo -u postgres psql -c "CREATE DATABASE stx" || true
printf "%s:%s:postgres:postgres:%s\n" "${POSTGRES_HOST}" "${POSTGRES_PORT}" "${POSTGRES_PASSWORD}" >> ${HOME}/.pgpass
printf "%s:%s:%s:%s:%s\n"            "${POSTGRES_HOST}" "${POSTGRES_PORT}" "${POSTGRES_DB}" "${USER}" "${POSTGRES_PASSWORD}" >> ${HOME}/.pgpass
chmod 600 ${HOME}/.pgpass

echo -e "\n8. Build and install cJSON\n"
cd ${HOME}
git clone https://github.com/DaveGamble/cJSON.git
mkdir -p cJSON/build && cd cJSON/build
cmake .. && make && sudo make install
cd ${HOME} && rm -rf cJSON/

echo -e "\n9. Clone stx project from GitHub\n"
cd ${HOME}
git clone https://github.com/ZX2XZX2/stx.git || (cd stx && git pull)

echo -e "\n10. Install Java (OpenJDK 21 LTS) and PostgreSQL JDBC driver\n"
sudo apt install -y openjdk-21-jdk
grep -q 'JAVA_HOME' ${HOME}/.bashrc || \
    echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ${HOME}/.bashrc
mkdir -p ${JAVA_DIR}/output
cd ${JAVA_DIR}/output
curl -O https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar

echo -e "\n11. Populate the database\n"
cd ${SHELL_DIR}
export DB_BACKUP_DIR=${DB_BKP_FILE}
./restore_db.sh stx stx

echo -e "\n12. Generate stx_cfg.ini\n"
cat > ${HOME}/stx_cfg.ini <<EOF
[datafeed]
data_dir = ${DATA_DIR}
download_dir = ${DOWNLOAD_DIR}
[postgres_db]
db_name = ${POSTGRES_DB}
usb_1 = ${USB_1}
usb_2 = ${USB_2}
usb_3 = ${USB_3}
EOF

cd ${CRT_DIR}
echo -e "\nSystem installation complete."
echo -e "\nNext steps:"
echo -e "  1. Run python_install.sh to set up Python 3.12 and the virtual environment"
echo -e "  2. Add the following to /etc/sudoers for DB backups:"
echo -e "       ${USER}  ALL=(ALL) NOPASSWD: /usr/bin/pg_dump\n"
