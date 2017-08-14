#!/bin/sh
spark-submit --master yarn-client ngramQ1.py hdfs://hadoop2-0-0/data/1gram/*
