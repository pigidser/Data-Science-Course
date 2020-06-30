#!/bin/bash

tabName=$1
dbName=$2

echo Executing data pipe loading...

./sqoop_import movies test

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
echo Done
