#!/bin/sh

python3 mr/message_word_count_preprocessed.py -c mrjob.conf -r hadoop hdfs:///user/hadoop/emails_preprocessed.csv