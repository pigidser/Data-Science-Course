#!/bin/bash

file_name="books"
local_file_path="Books_Data_Clean.csv"
hdfs_file_path="/user/hadoopuser/books"
db_name="test"
ext_tab_name="ext_books"
tab_orc_name="books_orc"
tab_parquet_name="books_parquet"
tab_avro_name="books_avro"

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

# Drop tables
echo "drop table if exists ${db_name}.${tab_orc_name} purge;" > sql.txt
echo "drop table if exists ${db_name}.${tab_parquet_name} purge;" >> sql.txt
echo "drop table if exists ${db_name}.${tab_avro_name} purge;" >> sql.txt
echo "drop table if exists ${db_name}.${ext_tab_name} purge;" >> sql.txt
# create temp table
echo "create external table ${db_name}.${ext_tab_name} (year int,book_name string,author string,language_code string," >>sql.txt
echo "author_rating string,book_average_rating string,book_ratings_count string,genre string,gross_sales string," >>sql.txt
echo "publisher_revenue string,sale_price string,sales_rank string,publisher string,units_sold string)" >>sql.txt
echo "row format delimited fields terminated by ',' location '${hdfs_file_path}' tblproperties('skip.header.line.count'='1');" >> sql.txt
# create tables
echo "create table ${db_name}.${tab_orc_name} stored as ORC as select * from ${db_name}.${ext_tab_name};" >> sql.txt
echo "create table ${db_name}.${tab_parquet_name} stored as PARQUET as select * from ${db_name}.${ext_tab_name};" >> sql.txt
echo "create table ${db_name}.${tab_avro_name} stored as AVRO as select * from ${db_name}.${ext_tab_name};" >> sql.txt
# drop temp table
echo "drop table ${db_name}.${ext_tab_name}" >> sql.txt

./exec_hive_cmd.sh "$(<sql.txt)"
if [ $? -ne 0 ]
then
  echo "Error during executing sql.txt"
  exit 1
fi

rm sql.txt

remove_dir

