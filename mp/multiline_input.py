from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMultilineInput(MRJob):

    def mapper_count_init(self):
        self.receiver = ''
        self.sender = ''
        self.in_header = False

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_count_init,
                   mapper=self.mapper_count,
                   combiner=self.combiner_count,
                   reducer=self.reducer_count),
            MRStep(mapper=self.mapper_trans,
                   reducer=self.reducer_trans)
        ]

    def mapper_count(self, _, line):
        line = line.strip()

        if line:
            self.in_header = False
        elif ',"Message-ID:"' in line:
            self.in_header = True

        if not self.in_header:
            if line.find('To: ') == 0:
                line = line[4:].strip().replace(",", "").replace(
                    "<", "").replace(">", "").replace("\"", "").replace(
                        "\'", "").replace("\\", "")
                self.receiver = line.split(" ")

            if line.find('From: ') == 0:
                self.sender = line[6:]

            if self.sender and len(self.receiver) > 0:
                for recv in self.receiver:
                    yield (self.sender, recv), 1
                self.receiver = ''
                self.sender = ''

    def combiner_count(self, pair, count):
        yield pair, sum(count)

    def reducer_count(self, pair, count):
        yield pair, sum(count)

    def mapper_trans(self, pair, count):
        sender = pair[0]
        receiver = pair[1]
        yield sender, (receiver, count)

    def reducer_trans(self, sender, recv_list):
        sorted_list = sorted(list(recv_list), key=lambda recv: -recv[1])
        if len(sorted_list) >= 3:
            yield sender, [recv for (recv, count) in sorted_list[:3]]

if __name__ == '__main__':
    MRMultilineInput.run()
