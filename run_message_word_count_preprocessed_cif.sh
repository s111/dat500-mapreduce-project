#!/bin/sh

python3 mr/message_word_count_cif.py -c mrjob.conf -r hadoop hdfs:///user/hadoop/emails.csv
