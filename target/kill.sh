#!/bin/bash
for pid in $(ps -ef | grep adtstreaming-1.0-SNAPSHOT-jar-with-dependencies.jar | grep -v grep | cut -c 9-15);do
	echo kill $pid
	kill -9 $pid
done
