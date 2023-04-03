#!/bin/bash

ops='port:,realtime'
USAGE="\nUsage: $0 [--port port-number] [--realtime] \n"
OPTIONS=$(getopt --options '' --longoptions ${ops} --name "$0" -- "$@")
[[ $? != 0 ]] && exit 3
eval set -- "${OPTIONS}"

FLASK_PORT=5000
STX_REALTIME=0

while true
do
    case "${1}" in
	--port)
	    FLASK_PORT="$2"
	    shift 2
	    ;;
    --realtime)
        FLASK_PORT=5555
        STX_REALTIME=1
        shift
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

export CRT_DIR=${PWD}
source ${HOME}/.env_vars
source ${HOME}/.envs/stx/bin/activate
BASE_DIR=${HOME}/stx
cd ${BASE_DIR}/python
export LANG=en_US.UTF-8
export FLASK_APP=stxws
export FLASK_ENV=development
flask run -p ${FLASK_PORT}
cd ${CRT_DIR}
