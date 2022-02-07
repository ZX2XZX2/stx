#!/bin/bash

################################################################
# This script allows to visualize the last generated PDF report.
# The intraday report will be copied in a temporary directory.
# Viewing the report will not prevent it from refreshing
################################################################

export CRT_DIR=${PWD}
export MARKET_DIR=${HOME}/market
export TMP_DIR=${HOME}/temp
mkdir -p ${TMP_DIR}
cd ${MARKET_DIR}
PDF_FILE=$(ls -t *.pdf | head -n 1)
cp ${MARKET_DIR}/${PDF_FILE} ${TMP_DIR}
evince ${TMP_DIR}/${PDF_FILE}
rm ${TMP_DIR}/${PDF_FILE}
