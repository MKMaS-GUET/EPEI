#!/bin/bash

# set query path parameter, {query_file_folder}/{query_file_type}{query_file_num}{query_file_suffix}
query_file_folder=/media/lyp/disk/dataset/wiki/queries/queries          # query file folder
query_file_type=(J3 J4 P2 P3 P4 S1 S2 S3 S4 T2 T3 T4 TI2 TI3 TI4 Tr1 Tr2)            # query file type (e.g. `(C L SF S)` for WatDiv), or file suffix (e.g. `(lubm_q)` for LUBM)
query_file_suffix=          # the suffix of the query file (e.g. `.in`, `.sql`, etc)

# specify RDF database name
database_name=wiki              # database name

# binary executable for query
EXE=./epei

# set output folder, {result_output_folder}/{query_file_type}{query_file_num}{output_file_suffix}
result_output_folder=./result/wiki       # the path of the output file folder
output_file_suffix=.txt         # the suffix of the output files (e.g. `.txt`, etc)

mkdir -p $result_output_folder


for file_type in ${query_file_type[@]}
do
    echo 'Query Type: ' $file_type
    file_path="${query_file_folder}/${file_type}${i}${query_file_suffix}"
    rslt_path="${result_output_folder}/${file_type}${output_file_suffix}"
    if [ -f $file_path ]; then
        echo query $database_name $file_path
        $EXE query -db $database_name -f $file_path >> $rslt_path
    else
        echo "$file_path does not exist."
    fi
    # sync
    # echo 3 > /proc/sys/vm/drop_caches
    # sleep 10
done
