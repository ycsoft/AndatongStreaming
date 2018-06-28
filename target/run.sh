#!/bin/sh
echo 'server starting..........'
echo 'set env' 
#export PYTHONPATH=/home/ubuntu/anaconda3/lib/python3.5/site-packages:/home/ubuntu/Fra-Back-End/fraplatformserver
#nohup python3.5 -u  fraplatformserver.py > logs 2>&1 & 
#/usr/bin/python3.5 -u  ./fraplatformserver.py 
nohup spark-submit --master yarn --executor-memory 2g --num-executors 4 --driver-memory 4g --class com.unidt.streaming.AndatongStreaming  adtstreaming-1.0-SNAPSHOT-jar-with-dependencies.jar &
