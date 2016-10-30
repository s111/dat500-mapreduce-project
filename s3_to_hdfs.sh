#!/bin/sh

# Move files from s3 into hdfs
hadoop fs -cp s3://dat500/emails.csv /user/hadoop/
hadoop fs -cp s3://dat500/emails_preprocessed.csv /user/hadoop/
