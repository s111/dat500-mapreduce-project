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
        self.words = {}

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
                     for word in re.split(DELIMITERS, "".join(self.body))]

            for word in words:
                if word not in self.words:
                    self.words[word] = 0

                self.words[word] += 1

            self.body = []

    def mapper_final(self):
        words = set(self.words.keys()).intersection(self.wordset)

        for word in words:
            yield word, self.words[word]

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
