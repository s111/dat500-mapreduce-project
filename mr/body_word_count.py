from mrjob.job import MRJob
from nltk.corpus import words

import re

MESSAGE_ID = '","Message-ID: '
FILTER = "<> \\\"\'"
DELIMITERS = '\.|,| '


class MRBodyWordCount(MRJob):
    def mapper_init(self):
        self.in_body = False
        self.body = []
        self.wordset = set(words.words())

    def mapper(self, _, line):
        line = line.strip()

        if MESSAGE_ID in line:
            self.in_body = False
        elif not line:
            self.in_body = True

        if self.in_body:
            self.body.append(line)
        else:
            # add some form of filtering
            words = [word.strip(FILTER)
                     for word in re.split(DELIMITERS, "".join(self.body))
                     if word.strip(FILTER) in self.wordset]

            self.body = []
            count = {}

            for word in words:
                if word in count:
                    count[word] += 1
                else:
                    count[word] = 1

            for k, v in count.items():
                yield k, v

    def combiner(self, word, count):
        count = sum(count)
        if count >= 10:
            yield word, count

    def reducer(self, word, count):
        count = sum(count)
        if count >= 10:
            yield word, count


if __name__ == '__main__':
    MRBodyWordCount.run()
