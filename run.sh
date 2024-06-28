#! /bin/bash

sudo hdfs dfs -mkdir /corpus
sudo hdfs dfs -put static/corpus/* /corpus

sudo hive -f inverted_index.hql

sudo hdfs dfs -ls /user/hadoop/logs

sudo hdfs dfs -get /user/hadoop/logs/000000_0 .

ls

source env/bin/activate
python server.py
