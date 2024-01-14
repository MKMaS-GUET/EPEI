#!/bin/bash

# set query path parameter, {query_file_folder}/{query_file_type}{query_file_num}{query_file_suffix}
query_file_folder=/media/lyp/disk/dataset/wiki/queries          # query file folder
query_file_type=(J3-queries J4-queries P2-queries P3-queries P4-queries S1-queries S2-queries S3-queries S4-queries T2-queries T3-queries T4-queries TI2-queries TI3-queries TI4-queries Tr1-queries Tr2-queries)            # query file type (e.g. `(C L SF S)` for WatDiv), or file suffix (e.g. `(lubm_q)` for LUBM)
query_file_suffix=          # the suffix of the query file (e.g. `.in`, `.sql`, etc)

# specify RDF database name
database_name=wiki_1              # database name

# binary executable for query
EXE=./hsindb

# set output folder, {result_output_folder}/{query_file_type}{query_file_num}{output_file_suffix}
result_output_folder=./wiki_result       # the path of the output file folder
output_file_suffix=.txt         # the suffix of the output files (e.g. `.txt`, etc)

mkdir -p $result_output_folder


for file_type in ${query_file_type[@]}
do
    # echo 'Query Type: ' $file_type
    # file_path="${query_file_folder}/${file_type}${i}${query_file_suffix}"
    # rslt_path="${result_output_folder}/${file_type}${i}${output_file_suffix}"
    # rslt_path="${result_output_folder}/${file_type}${output_file_suffix}"
    # if [ -f $file_path ]; then
    #     echo query $database_name $file_path
        # $EXE $database_name $file_path > $rslt_path
        # $EXE query -db $database_name -f $file_path >> $rslt_path
    # else
    #     echo "$file_path does not exist."
    # fi
    $EXE sync
    # $EXE 'echo 3 > /proc/sys/vm/drop_caches'
    sleep 10
done
