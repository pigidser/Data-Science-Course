#!/bin/bash

file_name="movies"
local_file_path="/home/hadoopuser/Documents/movies.csv"
hdfs_file_path="/user/hadoopuser/movies"
db_name="test"
ext_tab_name="ext_movies"
tab_name="movies_orc"

function create_dir {
  hdfs dfs -mkdir -p $hdfs_file_path
}

function save_file {
  hdfs dfs -put $local_file_path $hdfs_file_path
}

function remove_dir {
  hdfs dfs -rm -r -f -skipTrash $hdfs_file_path
}

create_dir
save_file

# Create external table
echo "drop table if exists ${db_name}.${tab_name} purge;" > sql.txt
echo "drop table if exists ${db_name}.${ext_tab_name} purge;" >> sql.txt
echo "create external table ${db_name}.${ext_tab_name} (movieId int, title string, genres string)" >> sql.txt
echo "row format delimited fields terminated by ',' location '${hdfs_file_path}' tblproperties('skip.header.line.count'='1');" >> sql.txt
echo "create table ${db_name}.${tab_name} stored as ORC as select * from ${db_name}.${ext_tab_name};" >> sql.txt
echo "drop table ${db_name}.${ext_tab_name}" >> sql.txt

./exec_hive_cmd.sh "$(<sql.txt)"
if [ $? -ne 0 ]
then
  echo "Error during executing sql.txt"
  exit 1
fi

rm sql.txt

remove_dir

