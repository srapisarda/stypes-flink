#!/bin/bash

rm -f stats.csv
files=($(ls *.csv))
for file in "${files[@]}"
do
#echo $file
rows=$(wc -l $file | awk '{print $1;}')
#echo $rows
bytes=$(wc -c $file | awk '{print $1;}')
line="$file,$((rows)),1,$bytes,$((bytes*2))"
echo $line >> stats.csv
done