#!/bin/bash

tabName=$1
dbName=$2

echo -n Importing $tabName table into $dbName database with sqoop import command... 

# Clear $tabName folder in hdfs
hdfs dfs -rm -r -f -skipTrash $tabName >clear_out.txt 2>clear_err.txt

# Import
sqoop import --connect jdbc:mysql://hadoop-master/pigidser \
--username hadoop \
--password-file file:///$HOME/.mysql_password \
--hive-import -m 1 \
--table $tabName \
--hive-table ${dbName}.${tabName} \
--hive-overwrite \
>sqoop_out.txt 2>sqoop_err.txt

echo Done!
