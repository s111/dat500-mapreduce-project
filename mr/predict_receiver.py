from mrjob.job import MRJob
from mrjob.step import MRStep


class MRPredictReceiver(MRJob):
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_to_from_init,
                   mapper=self.mapper_from_to,
                   combiner=self.combiner_from_to,
                   reducer=self.reducer_from_to),
            MRStep(mapper=self.mapper_predict,
                   reducer=self.reducer_predict)
        ]

    def clean_address(self, address):
        return address.replace("<", "").replace(">", "").replace(
            "\\", "").replace("\"", "")

    def mapper_to_from_init(self):
        self.in_body = False
        self.sender = ""

    def mapper_from_to(self, _, line):
        line = line.strip()

        if '","Message-ID: ' in line:
            self.in_body = False
        elif not line:
            self.in_body = True

        if not self.in_body:
            if line.startswith("From:"):
                self.sender = self.clean_address(line.split()[1])

            if line.startswith("To:"):
                mails = line[4:].split(',')

                for mail in mails:
                    mail = mail.split(' ')

                    if len(mail) > 1:
                        mail = mail[-1]
                    else:
                        mail = mail[0]

                    if mail:
                        yield (self.sender, self.clean_address(mail)), 1

    def combiner_from_to(self, key, values):
        yield key, sum(values)

    def reducer_from_to(self, key, values):
        yield key, sum(values)

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
