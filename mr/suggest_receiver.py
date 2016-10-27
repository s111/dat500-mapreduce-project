from mrjob.job import MRJob
from mrjob.step import MRStep
import re

MESSAGE_ID = "\",\"Message-ID: "
EMAIL_REGX = re.compile(r"[^@]+@[^@]+\.[^@]+")

def clean_address(address):
    return re.sub('[<>,\'/\\\\"]', '', address)

def valid_address(addresses):
    return [addr for addr in addresses if EMAIL_REGX.match(addr)]

class MRSuggestReceiver(MRJob):
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_to_from_init,
                   mapper=self.mapper_from_to,
                   mapper_final=self.mapper_to_from_final,
                   reducer=self.reducer_from_to),
            MRStep(mapper=self.mapper_recv_pred,
                   reducer=self.reducer_recv_pred),
            MRStep(mapper=self.mapper_join,
                   reducer=self.reducer_join),
            MRStep(mapper=self.mapper_reassemble,
                   reducer=self.reducer_reassemble)
        ]

    def mapper_to_from_init(self):
        self.in_header = False
        self.sender = ""
        self.receiver = []
        self.map = {}

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
                if self.sender in self.map:
                    receivers = self.map[self.sender]
                else:
                    receivers = {}

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

    def mapper_recv_pred(self, key, count):
        sender = key[0]
        receiver = key[1]
        yield sender, (receiver, count)

    def reducer_recv_pred(self, sender, receiver_count):
        sorted_list = sorted(list(receiver_count), key=lambda second: -second[1])
        if len(sorted_list) >= 3:
            yield sender, [receiver for (receiver, count) in sorted_list[:3]]

    def mapper_join(self, sender, receivers):
        receivers = list(receivers)
        yield sender, receivers
        for recv in receivers:
            yield recv, [sender]

    def reducer_join(self, email, links):
        links = list(links)
        metadata = max(enumerate(links), key = lambda tup: len(tup[1]))
        email_recv_pred = metadata[1]
        # If there is not enough mail sent by email to have a top 3 list from second step, abort.
        if len(email_recv_pred) == 3:
            del links[metadata[0]]
            for link in links:
                yield link[0], email_recv_pred

    def mapper_reassemble(self, sender, suggestions):
        suggestions = list(suggestions)
        if sender in suggestions:
            suggestions.remove(sender)
        yield sender, suggestions

    def reducer_reassemble(self, sender, suggestions):
        tmp = []
        for l in list(suggestions):
            tmp += l
        yield sender, list(set(tmp))

if __name__ == '__main__':
    MRSuggestReceiver.run()
