#!/bin/sh

python3 mr/message_word_count.py -c mrjob.conf -r hadoop hdfs:///user/hadoop/emails.csv
