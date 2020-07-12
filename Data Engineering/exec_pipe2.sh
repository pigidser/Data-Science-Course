#!/bin/bash

tabName=$1
dbName=$2

echo Executing data pipe loading...

#./sqoop_import movies test

./exec_hive_cmd.sh "drop table if exists ${tabName}_orc purge"
if [ $? -ne 0 ]
then
  echo "Error during executing sql"
  exit 1
fi

./exec_hive_cmd.sh "c1reate table ${tabName}_orc stored as ORC as select * from test.${tabName}"
if [ $? -ne 0 ]
then
  echo "Error during executing sql"
  exit 1
fi

#execHiveCmd("drop table " + hiveTab + " purge")

echo Done!
exit 0
