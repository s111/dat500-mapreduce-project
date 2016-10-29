#!/bin/sh

python3 mr/message_word_count.py -c mrjob_nltk.conf -r hadoop hdfs:///user/hadoop/emails.csv
