#!/bin/bash
declare -a row_keys

hdfs_path=${1}
table=${2}

echo $hdfs_path
echo $table

while IFS= read -r line; do
  row_keys+=("$line")
done < hdfs dfs -cat ${1}

for row_key in "${row_keys[@]}"
do
  echo "Deleting key: ${row_key}"
  echo -e "deleteall '$table' , '$rowkey' "  | hbase shell
done