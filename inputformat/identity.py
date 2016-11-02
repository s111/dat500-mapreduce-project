from mrjob.job import MRJob
from mrjob.protocol import RawProtocol


class MRIdentity(MRJob):
    HADOOP_INPUT_FORMAT = "com.sebastianpedersen.hadoop.mapred.MessageInputFormat"
    INPUT_PROTOCOL = RawProtocol

    def mapper(self, key, value):
        self.increment_counter("LINES", "MESSAGES", 1)

        yield key, value


if __name__ == "__main__":
    MRIdentity.run()
