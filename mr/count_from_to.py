from mrjob.job import MRJob


class MRCountDomains(MRJob):
    def clean_address(self, address):
        return address.replace("<", "").replace(">", "").replace(
            "\\", "").replace("\"", "")

    def mapper_init(self):
        self.in_body = False
        self.sender = ""

    def mapper(self, _, line):
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

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRCountDomains.run()
