#!/bin/sh

source ~/rootkey.csv

export AWS_ACCESS_KEY_ID=${AWSAccessKeyId/%$'\r'/}
export AWS_SECRET_ACCESS_KEY=${AWSSecretAccessKey/%$'\r'/}

export CLUSTER_ID=`mrjob create-cluster -c mrjob_emr.conf`
