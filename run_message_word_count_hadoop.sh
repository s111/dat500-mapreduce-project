#!/bin/sh

python3 mr/message_word_count.py -c mrjob_emr.conf --cluster-id $CLUSTER_ID -r emr s3://dat500/emails.csv
