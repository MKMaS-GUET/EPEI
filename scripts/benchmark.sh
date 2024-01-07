#!/bin/bash

# set query path parameter, {query_file_folder}/{query_file_type}{query_file_num}{query_file_suffix}
query_file_folder=/home/lyp/Documents/dataset/queries/WatDiv/          # query file folder
query_file_type=(C L SF S)            # query file type (e.g. `(C L SF S)` for WatDiv), or file suffix (e.g. `(lubm_q)` for LUBM)
query_file_suffix=.in          # the suffix of the query file (e.g. `.in`, `.sql`, etc)
query_file_num=20             # the query file number

# specify RDF database name
database_name=100m              # database name

# binary executable for query
EXE=./hsindb

# set output folder, {result_output_folder}/{query_file_type}{query_file_num}{output_file_suffix}
result_output_folder=./result       # the path of the output file folder
output_file_suffix=.txt         # the suffix of the output files (e.g. `.txt`, etc)

#if [ -d $result_output_folder ]; then
#    rm -rf $result_output_folder
#fi

mkdir -p $result_output_folder


for file_type in ${query_file_type[@]}
do
    echo 'Query Type: ' $file_type
    for (( i = 1; i <= query_file_num; i++ ))
    do
        file_path="${query_file_folder}/${file_type}${i}${query_file_suffix}"
        # rslt_path="${result_output_folder}/${file_type}${i}${output_file_suffix}"
        rslt_path="${result_output_folder}/${file_type}${output_file_suffix}"
        if [ -f $file_path ]; then
            echo $EXE query $database_name $file_path
            # $EXE $database_name $file_path > $rslt_path
            echo "-----------------------------------------" >> $rslt_path
            echo ${file_type}${i}${query_file_suffix} >> $rslt_path
            start_time=$(date +%s%N)
	        $EXE query -n $database_name -f $file_path >> $rslt_path
            end_time=$(date +%s%N)
		    result=$(( (end_time - start_time) / 1000 ))
		    echo $result 
        else
            echo "$file_path does not exist."
        fi
        if [ $((i % 10)) -eq 0 ]; then
            sleep 10
        fi
    done
done
