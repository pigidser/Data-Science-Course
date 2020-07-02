#!/bin/bash

local_file_path="/home/hadoopuser/Documents/movies.csv"
hdfs_file_path="/user/hadoopuser/movies"

hdfs dfs -rm -r -f $hdfs_file_path
hdfs dfs -mkdir -p $hdfs_file_path
hdfs dfs -put $local_file_path $hdfs_file_path
