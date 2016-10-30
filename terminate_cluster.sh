#!/bin/sh

mrjob create-cluster -c mrjob_emr.conf --terminate-cluster $CLUSTER_ID
