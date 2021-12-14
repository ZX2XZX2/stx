#!/bin/bash

# TODO:
# 1. The script does not exit if the parameters are not setup

# Remember the current directory to return at the end of script execution
CRT_DIR=$(dirname $0)
cd "${CRT_DIR}" && CRT_DIR=$PWD

echo -e "1. Make .ssh directory (if not there) and copy SSH keys\n\n"
mkdir -p ${HOME}/.ssh
mv ${HOME}/id_ed25519 ${HOME}/.ssh
mv ${HOME}/id_ed25519.pub ${HOME}/.ssh
chmod 600 ${HOME}/.ssh/id_ed25519

# Print script usage
USAGE="\nUsage: $0 [--env-vars-file /path/to/env_vars_file] [--postgres-version 13]\n"

# Parse the input arguments
ops='env-vars-file:,postgres-version:'
declare {ENV_VARS_FILE,POSTGRES_VERSION}=''
OPTIONS=$(getopt --options '' --longoptions ${ops} --name "$0" -- "$@")
[[ $? != 0 ]] && exit 3

eval set -- "${OPTIONS}"

while true
do
    case "${1}" in
	--env-vars-file)
	    ENV_VARS_FILE="$2"
	    shift 2
	    ;;
	--postgres-version)
	    POSTGRES_VERSION="$2"
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

echo -e "2. Check that environment variables file and Postgres version was specified\n\n"
[[ "${ENV_VARS_FILE}" == '' ]] && (echo -e "\nError: no environment variables file specified! Check script usage.\n${USAGE}" && exit 1)
[[ "${POSTGRES_VERSION}" == '' ]] && (echo -e "\nError: no postgres version specified! Check script usage.\n${USAGE}" && exit 1)

echo -e "Running script with the following parameters:"
echo -e "  Environment variables file: ${ENV_VARS_FILE}"
echo -e "  Postgres version: ${POSTGRES_VERSION}"

echo -e "3. Read the environment variables from file.  TODO: check that all the environment variables that are needed have been specified\n\n"
source ${ENV_VARS_FILE}

echo -e "4. Install python development tools\n\n"
sudo apt update
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
sudo apt install -y git python3-pip python3-dev build-essential emacs libssl-dev libffi-dev
sudo apt -y autoremove

echo -e "5. Install and activate virtualenv\n\n"
sudo pip3 install virtualenv virtualenvwrapper
mkdir -p ${HOME}/.envs
echo 'export VIRTUALENVWRAPPER_PYTHON=$(which python3)' >> ${HOME}/.bashrc
echo 'export WORKON_HOME=${HOME}/.envs' >> ${HOME}/.bashrc
echo 'source /usr/local/bin/virtualenvwrapper.sh' >> ${HOME}/.bashrc
source ${HOME}/.bashrc

echo -e "6. Configure git global email and name\n\n"
git config --global user.name "${GIT_USERNAME}"
git config --global user.email "${GIT_EMAIL}"

echo -e "7. Install Postgres\n\n"
sudo apt -y install vim bash-completion wget
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RELEASE=$(lsb_release -cs)
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${RELEASE}"-pgdg main | sudo tee  /etc/apt/sources.list.d/pgdg.list
cat /etc/apt/sources.list.d/pgdg.list
sudo apt update
sudo apt install -y postgresql-${POSTGRES_VERSION} postgresql-client-${POSTGRES_VERSION}
systemctl status postgresql.service --no-pager
systemctl status postgresql@${POSTGRES_VERSION}-main.service --no-pager

echo -e "8. Create postgres users and database\n\n"

sudo -u postgres psql -d postgres -c "ALTER USER postgres PASSWORD '${POSTGRES_PASSWORD}'"
sudo -u postgres psql -d postgres -c "CREATE ROLE ${USER} WITH LOGIN SUPERUSER PASSWORD '${POSTGRES_PASSWORD}'"
sudo -u postgres psql -c "CREATE DATABASE stx"
echo "${POSTGRES_HOST} ${POSTGRES_PORT} postgres postgres ${POSTGRES_PASSWORD}\n" >> ${HOME}/.pgpass
echo "${POSTGRES_HOST} ${POSTGRES_PORT} ${POSTGRES_DB} ${USER} ${POSTGRES_PASSWORD}\n" >> ${HOME}/.pgpass
chmod 600 ${HOME}/.pgpass

echo -e "9. Install Postgres dev library\n\n"
sudo apt install -y libpq-dev
sudo apt install -y libcurl4-openssl-dev

echo -e "10. Install cmake and cJSON\n\n"
sudo apt install -y cmake
cd ${HOME}
git clone https://github.com/DaveGamble/cJSON.git
cd cJSON/
mkdir build
cd build
cmake ..
make
sudo make install
cd ${HOME}
rm -rf cJSON/

echo -e "11. Create stx virtual environment\n\n"
source ${HOME}/.bashrc
cd ${HOME}/.envs
virtualenv stx

echo -e "12. Get the project code from girhub\n\n"
cd ${HOME}
git clone https://github.com/ZX2XZX2/stx.git

cd ${CRT_DIR}
