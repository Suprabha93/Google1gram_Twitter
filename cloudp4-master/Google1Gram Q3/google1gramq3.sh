#!/bin/sh
spark-submit --master yarn-client avgWordLength.py hdfs://hadoop2-0-0/data/1gram/*