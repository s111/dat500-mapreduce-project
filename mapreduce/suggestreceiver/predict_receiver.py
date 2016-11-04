import re
from collections import defaultdict
from collections import Counter

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

from protocol.csv_output_protocol import CSVOutputProtocol

RECORD_DELIMITER = "\30"
SENDER = "From: "
RECEIVERS = "To: "
EMAIL = re.compile(r"[^@]+@[^@]+\.[^@]+")


def clean_address(address):
    return re.sub("[<>,\'/\\\\\"]", "", address)


def valid_address(addresses):
    return [addr for addr in addresses if EMAIL.match(addr)]


class MRPredictReceiver(MRJob):
    HADOOP_INPUT_FORMAT = ("com.sebastianpedersen"
                           ".hadoop.mapred.MessageInputFormat")
    INPUT_PROTOCOL = RawProtocol

    def configure_options(self):
        super(MRPredictReceiver, self).configure_options()

        self.add_passthrough_option(
            "--output-csv",
            action="store_true",
            dest="output_csv",
            help="Output csv for consumption by Hive")

    def output_protocol(self):
        if self.options.output_csv:
            return CSVOutputProtocol()

        return super(MRPredictReceiver, self).output_protocol()

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_to_from_init,
                   mapper=self.mapper_from_to,
                   mapper_final=self.mapper_to_from_final,
                   reducer=self.reducer_from_to),
            MRStep(mapper=self.mapper_predict,
                   reducer=self.reducer_predict)
        ]

    def mapper_to_from_init(self):
        self.map = defaultdict(Counter)

    def mapper_from_to(self, _, email):
        # extract from and to lines from email.
        s_start = email.find(SENDER) + len(SENDER)
        s_end = email.find(RECORD_DELIMITER, s_start)
        r_start = email.find(RECEIVERS, s_end) + len(RECEIVERS)
        r_end = email.find(RECORD_DELIMITER, r_start)

        sender = clean_address(email[s_start:s_end])
        receivers = valid_address(
            clean_address(email[r_start:r_end]).split())

        self.map[sender].update(receivers)


    def mapper_to_from_final(self):
        for sender, receivers in self.map.items():
            for receiver, count in receivers.items():
                yield (sender, receiver), count

    def reducer_from_to(self, pair, count):
        yield pair, sum(count)

    def mapper_predict(self, pair, count):
        sender, receiver = pair
        yield sender, (receiver, count)

    def reducer_predict(self, sender, receiver_count):
        sorted_list = sorted(
            list(receiver_count), key=lambda second: -second[1])
        if len(sorted_list) >= 3:
            yield sender, [receiver for (receiver, count) in sorted_list[:3]]


if __name__ == "__main__":
    MRPredictReceiver.run()
