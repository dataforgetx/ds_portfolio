#!/bin/bash

export PATH=/usr/local/bin:$PATH
export ORACLE_HOME=`dbhome default`
export LD_LIBRARY_PATH=$ORACLE_HOME/lib

export USER_NAME=caps
export DB=qawh
export USER_PASSWORD=`op -g dba $USER_NAME@$DB`

python send2dps.py "$@"