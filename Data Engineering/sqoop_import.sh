#!/bin/bash

tabName=$1
dbName=$2

echo Importing $tabName table into $dbName database with sqoop import command

# Clear $tabName folder in hdfs
hdfs dfs -rm -r -f -skipTrash $tabName

# Import
sqoop import --connect jdbc:mysql://hadoop-master/pigidser \
--username hadoop \
--password-file file:///$HOME/.mysql_password \
--hive-import -m 1 \
--table $tabName \
--hive-table ${dbName}.${tabName} \
--hive-overwrite

echo Done!
