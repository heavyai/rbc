#!/bin/bash
#
# Run omniscidb server from conda environment (requires linux)
#
# Usage:
#
#  $ bash start_omniscidb.sh                # -> cpu or cuda 10.2
#  $ CUDA_VER=none bash start_omniscidb.sh  # -> cpu
#  $ CUDA_VER=cpu bash start_omniscidb.sh   # -> cpu-only
#  $ CUDA_VER=9.2 bash start_omniscidb.sh   # -> cudatoolkit 9.2 from /usr/local/cuda-9.2*
#  $ CUDA_VER=9.2 USE_CUDA=conda bash start_omniscidb.sh  # -> condatoolkit 9.2 from conda-forge
#
# Environment variables:
#
#  CUDA_VER - specify CUDA version (9.2/10.0/10.1/10.2/), default is
#             10.2. When not set, run the server in CPU-only mode.
#
#  USE_CUDA - specify the source of cudatoolkit, conda or path to bash
#             script that initilizes the environment. Default is
#             /usr/local/cuda-$CUDA_VER*/env.sh.
#
# To update omniscidb, remove the corresponding conda environment
#
# Author: Pearu Peterson
# Created: June 2020
#

OMNISCIDB_OPTIONS=""
OMNISCIDB_OPTIONS="$OMNISCIDB_OPTIONS --enable-runtime-udf --enable-table-functions"

CUDA_VER="${CUDA_VER:-10.2}"


if [[ "$CUDA_VER" == "none" ]]
then
    ENV_KIND="cpu"
    USE_CUDA="${USE_CUDA:-conda}"
else
    if [[ -x "$(command -v nvidia-smi)" ]]
    then
        if [[ "$CUDA_VER" == "cpu" ]]
        then
            ENV_KIND="cpu-only"
            USE_CUDA="${USE_CUDA:-conda}"
            OMNISCIDB_OPTIONS="$OMNISCIDB_OPTIONS --cpu-only"
        else
            CUDA_ENV_SH="$(ls /usr/local/cuda-$CUDA_VER*/env.sh 2> /dev/null || exit 0)"
            if [[ "$USE_CUDA" != "conda" && "$CUDA_ENV_SH" != "" && -f "$CUDA_ENV_SH" ]]
            then
                ENV_KIND="$(basename $(dirname $CUDA_ENV_SH))"
                USE_CUDA="${USE_CUDA:-$CUDA_ENV_SH}"
            else
                ENV_KIND="cuda-$CUDA_VER"
                USE_CUDA="${USE_CUDA:-conda}"
            fi
        fi
    else
        ENV_KIND="cpu"
        USE_CUDA="${USE_CUDA:-conda}"
    fi
fi

USE_ENV="${USE_ENV:-omniscidb-$ENV_KIND-for-rbc}"

echo "USE_ENV=$USE_ENV CUDA_VER=$CUDA_VER USE_CUDA=$USE_CUDA"


CONDA_ENV_LIST=$(conda env list | awk '{print $1}' )

if [[ $CONDA_ENV_LIST = *"$USE_ENV"* ]]
then
    if [[ "$CONDA_DEFAULT_ENV" = "$USE_ENV" ]]
    then
        echo "deactivating $USE_ENV"
        conda deactivate
    fi
else
    if [[ "$ENV_KIND" == "cpu" ]]
    then
        conda create -c conda-forge -n $USE_ENV "omniscidb>=5.2.2=*cpu"  || exit 1
    else
        if [[ "$USE_CUDA" != "conda" && "$USE_CUDA" != "" && -f "$USE_CUDA" ]]
        then
            source $USE_CUDA
        fi
        if [[ "$ENV_KIND" == "cpu-only" ]]
        then
            conda create -c conda-forge -n $USE_ENV "omniscidb>=5.2.2=*cuda" || exit 1
        else
            if [[ ("$USE_CUDA" == "conda" || "$USE_CUDA" == "") && "$CUDA_VER" != "cpu" ]]
            then
                conda create -c conda-forge -n $USE_ENV "omniscidb>=5.2.2=*cuda" "cudatoolkit=$CUDA_VER"  || exit 1
            else
                conda create -c conda-forge -n $USE_ENV "omniscidb>=5.2.2=*cuda"  || exit 1
            fi
        fi
    fi
fi

source $(dirname $(dirname $CONDA_EXE))/etc/profile.d/conda.sh
conda activate $USE_ENV  || exit 1

# conda overrides environment
if [[ "$USE_CUDA" != "conda" && "$USE_CUDA" != "" && -f "$USE_CUDA" ]]
then
    source $USE_CUDA
fi

WORKDIR=/tmp/$USE_ENV-workdir

if [[ ! -d "$WORKDIR/data/mapd_data" ]]
then
    mkdir -p $WORKDIR/data
    omnisci_initdb -f $WORKDIR/data
fi

OMNISCIDB_OPTIONS="$OMNISCIDB_OPTIONS --data $WORKDIR/data"

cat << EndOfMessage
nvcc=`which nvcc`

Executing (`omnisci_server --version`):

  omnisci_server $OMNISCIDB_OPTIONS

To stop the omnisci_server, press Ctrl-C :)
EndOfMessage

omnisci_server $OMNISCIDB_OPTIONS
