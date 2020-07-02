#!/bin/bash

tabName=$1
dbName=$2

echo Executing data pipe loading...

# Load data from MySQL database to Hadoop
./sqoop_import $tabName $dbName
if [ $? -ne 0 ]
then
  echo "Error during sqoop import"
  exit 1
fi

# Create ORC table from a temporary table and delete temp
echo "drop table if exists ${tabName}_orc purge;" >sql.txt
echo "create table ${tabName}_orc stored as ORC as select * from test.${tabName};" >>sql.txt
echo "drop table ${dbName}.${tabName}" >>sql.txt

./exec_hive_cmd.sh "$(<sql.txt)"
if [ $? -ne 0 ]
then
  echo "Error during executing sql"
  exit 1
fi

rm sql.txt

echo Data pipeline is finished!
