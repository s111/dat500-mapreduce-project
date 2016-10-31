from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict
import re

MESSAGE_ID = "\",\"Message-ID: "
EMAIL_REGX = re.compile(r"[^@]+@[^@]+\.[^@]+")

def clean_address(address):
    return re.sub('[<>,\'/\\\\"]', '', address)

def valid_address(addresses):
    return [addr for addr in addresses if EMAIL_REGX.match(addr)]

class MRPredictReceiver(MRJob):
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
        self.in_header = False
        self.sender = ""
        self.receiver = []
        self.map = defaultdict(dict)

    def mapper_from_to(self, _, line):
        line = line.strip()

        if MESSAGE_ID in line:
            self.in_header = True
        elif not line:
            self.in_header = False

        if self.in_header:
            if line.startswith("From: "):
                self.sender = clean_address(line[6:].strip())
            elif line.startswith("To: "):
                self.receiver = valid_address(clean_address(line[4:].strip()).split())

            if self.sender and len(self.receiver):
                receivers = self.map[self.sender]

                for recv in self.receiver:
                    if recv in receivers:
                        receivers[recv] += 1
                    else:
                        receivers[recv] = 1
                self.map[self.sender] = receivers
                self.sender = ""
                self.receiver = []

    def mapper_to_from_final(self):
        for sender, receivers in self.map.items():
            for recv, count in receivers.items():
                yield (sender, recv), count

    def reducer_from_to(self, pair, count):
        yield pair, sum(count)

    def mapper_predict(self, key, count):
        sender = key[0]
        receiver = key[1]
        yield sender, (receiver, count)

    def reducer_predict(self, sender, receiver_count):
        sorted_list = sorted(list(receiver_count), key=lambda second: -second[1])
        if len(sorted_list) >= 3:
            yield sender, [receiver for (receiver, count) in sorted_list[:3]]


if __name__ == '__main__':
    MRPredictReceiver.run()
