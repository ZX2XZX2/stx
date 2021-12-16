#!/bin/bash
source ${HOME}/.env_vars
cd output
java -cp .:postgresql-${POSTGRES_JDBC_VERSION}.jar gui.ACtxHD
cd ..
