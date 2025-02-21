#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "[ERROR] Please supply the path to the file in hdfs.";
    exit 1;
fi

TMP_FILE=cat_counts.txt

python first_runner.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop "$1" > $TMP_FILE && 
python second_runner.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop "$1" &&
rm $TMP_FILE
