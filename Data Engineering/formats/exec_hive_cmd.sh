#!/bin/bash

sql=$1

echo Executing the Hive command:
echo ${sql}

beeline \
-u jdbc:hive2://hadoop-master:10000 \
-n hadoopuser -w $HOME/.hive_password \
-e "$sql"
if [ $? -ne 0 ]
then
  echo Error in $0
  exit 1
fi
