#!/bin/sh

python3 mr/message_word_count_preprocessed.py -c mrjob_emr.conf --cluster-id $CLUSTER_ID -r emr s3://dat500/emails_preprocessed.csv
