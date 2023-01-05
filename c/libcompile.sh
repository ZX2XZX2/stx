#!/bin/bash

export SRC_FILE=$1
export LIB_FILE="${SRC_FILE/\.c/.so}"
export TGT_FILE="/usr/local/sbin/${LIB_FILE}"
echo -e "gcc -fPIC -shared -o ${LIB_FILE} ${SRC_FILE} -O3 -I. -I/usr/include/postgresql -lpq -lm -lcjson -lcurl"
gcc -fPIC -shared -o ${LIB_FILE} ${SRC_FILE} -O3 -I. -I/usr/include/postgresql -lpq -lm -lcjson -lcurl
echo -e "sudo cp ${LIB_FILE} ${TGT_FILE}"
sudo cp ${LIB_FILE} ${TGT_FILE}
