import re
from collections import Counter

import nltk
from mrjob.job import MRJob
from nltk.corpus import words

MESSAGE_ID = "\",\"Message-ID: "
WORD = re.compile("\w+")


class MRMessageWordCount(MRJob):
    def mapper_init(self):
        nltk.data.path.append("nltk_data")

        self.vocabulary = {word.lower(): None for word in set(words.words())}
        self.words = Counter()

        self.buffer_lines = False
        self.lines = []

    def mapper(self, _, line):
        if MESSAGE_ID in line:
            self.buffer_lines = False

            message = "".join(self.lines).lower()
            words = (term for term in WORD.findall(message)
                     if term in self.vocabulary)

            self.words.update(words)
            self.lines = []
        elif not line:
            self.buffer_lines = True
        elif self.buffer_lines:
            self.lines.append(line)

    def mapper_final(self):
        message = "".join(self.lines).lower()
        words = (term for term in WORD.findall(message)
                 if term in self.vocabulary)

        self.words.update(words)

        for word, occurences in self.words.items():
            yield word, occurences

    def reducer(self, word, occurences):
        count = sum(occurences)

        if count >= 10:
            yield word, count


if __name__ == "__main__":
    MRMessageWordCount.run()
