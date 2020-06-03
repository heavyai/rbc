#!/bin/bash
#
# Run omniscidb server from conda environment (requires linux)
#
# Usage:
#
#  $ USE_CUDA=0 bash start_omniscidb.sh
#  $ USE_CUDA=1 bash start_omniscidb.sh
#
# Environment variables:
#
#  USE_CUDA=0/1                 - use CUDA disabled/enabled server
#  USE_ENV=omniscidb-*-for-rbc  - create/use specified conda environment
#
# To update omniscidb, remove the corresponding conda environment
#
# Author: Pearu Peterson
# Created: June 2020
#

OMNISCIDB_OPTIONS=""
OMNISCIDB_OPTIONS="$OMNISCIDB_OPTIONS --enable-runtime-udf --enable-table-functions"

if [[ -x "$(command -v nvidia-smi)" ]]
then
    USE_CUDA="${USE_CUDA:-1}"
    if [[ "USE_CUDA" == "0" ]]
    then
        OMNISCIDB_OPTIONS="$OMNISCIDB_OPTIONS --cpu-only"
    fi
else
    USE_CUDA="${USE_CUDA:-0}"
fi

if [[ "USE_CUDA" == "1" ]]
then
    USE_ENV="${USE_ENV:-omniscidb-cuda-for-rbc}"
else
    USE_ENV="${USE_ENV:-omniscidb-cpu-for-rbc}"
fi

CONDA_ENV_LIST=$(conda env list | awk '{print $1}' )

if [[ $CONDA_ENV_LIST = *"$USE_ENV"* ]]
then
    if [[ "$CONDA_DEFAULT_ENV" = "$USE_ENV" ]]
    then
        echo "deactivating $USE_ENV"
        conda deactivate
    fi
else
    if [[ "USE_CUDA" == "1" ]]
    then
        conda create -c conda-forge -n $USE_ENV "omniscidb>=5.2.2=*cuda"
    else
        conda create -c conda-forge -n $USE_ENV "omniscidb>=5.2.2=*cpu"
    fi
fi

source $(dirname $(dirname $CONDA_EXE))/etc/profile.d/conda.sh
conda activate $USE_ENV

WORKDIR=/tmp/$USE_ENV-workdir

if [[ ! -d "$WORKDIR/data" ]]
then
    mkdir -p $WORKDIR/data
    omnisci_initdb -f $WORKDIR/data
fi

OMNISCIDB_OPTIONS="$OMNISCIDB_OPTIONS --data $WORKDIR/data"

cat << EndOfMessage
Executing:

  omnisci_server $OMNISCIDB_OPTIONS

To stop the omnisci_server, press Ctrl-C :)
EndOfMessage

omnisci_server $OMNISCIDB_OPTIONS
