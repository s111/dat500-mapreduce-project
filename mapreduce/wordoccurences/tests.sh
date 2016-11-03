#!/bin/sh

# We don't care about time. This is only see the data loss.
python3 yield_early.py --python-archive MRCount.py -r hadoop hdfs:///user/hadoop/emails.csv --total-only > count.txt
python3 in_mapper_combiner.py --python-archive MRCount.py -r hadoop hdfs:///user/hadoop/emails.csv --total-only >> count.txt
python3 in_mapper_combiner_valid_only.py --python-archive MRCount.py -r hadoop hdfs:///user/hadoop/emails.csv --total-only >> count.txt
python3 with_preprocessed_data.py --python-archive MRCount.py -r hadoop hdfs:///user/hadoop/emails_preprocessed.csv --total-only >> count.txt
python3 with_custom_format.py --python-archive MRCount.py -r hadoop hdfs:///user/hadoop/emails.csv --total-only >> count.txt

# perf is only used for running the command n times, and also for seeing the total time used.
# We inspect the logs for the actual time.
perf stat -r 3 -d python3 yield_early.py --python-archive MRCount.py -r hadoop hdfs:///user/hadoop/emails.csv --no-output
perf stat -r 3 -d python3 in_mapper_combiner.py --python-archive MRCount.py -r hadoop hdfs:///user/hadoop/emails.csv --no-output
perf stat -r 3 -d python3 in_mapper_combiner_valid_only.py --python-archive MRCount.py -r hadoop hdfs:///user/hadoop/emails.csv --no-output
perf stat -r 3 -d python3 with_preprocessed_data.py --python-archive MRCount.py -r hadoop hdfs:///user/hadoop/emails_preprocessed.csv --no-output
perf stat -r 3 -d python3 with_custom_format.py --python-archive MRCount.py -r hadoop hdfs:///user/hadoop/emails.csv --no-output
