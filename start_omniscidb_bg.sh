conda activate omnisci-ci
mkdir data && omnisci_initdb data -f
omnisci_server --version
omnisci_server --enable-runtime-udf --enable-table-functions 2>&1 > omniscidb-output.txt &
conda deactivate
