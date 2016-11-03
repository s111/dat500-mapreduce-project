import re
from collections import Counter

from nltk.corpus import words

from MRCount import MRCount

MESSAGE_ID = "\",\"Message-ID: "
WORD = re.compile("\w+")


class MRMessageWordCount(MRCount):
    def mapper_init(self):
        self.vocabulary = set(map(str.lower, words.words()))

        self.buffer_lines = False
        self.lines = []

    def getCount(self):
        message = "".join(self.lines).lower()
        words = (term for term in WORD.findall(message)
                 if term in self.vocabulary)

        return Counter(words)

    def mapper(self, _, line):
        if MESSAGE_ID in line:
            self.buffer_lines = False

            for word, occurences in self.getCount().items():
                yield self.getKey(word), occurences

            self.lines = []
        elif not line:
            self.buffer_lines = True
        elif self.buffer_lines:
            self.lines.append(line)

    def mapper_final(self):
        for word, occurences in self.getCount().items():
            yield self.getKey(word), occurences

    def combiner(self, word, occurences):
        yield word, sum(occurences)

    def reducer(self, word, occurences):
        yield word, sum(occurences)


if __name__ == "__main__":
    MRMessageWordCount.run()
