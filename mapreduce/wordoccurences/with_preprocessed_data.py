import re
from collections import Counter

from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from nltk.corpus import words

WORD = re.compile("\w+")


class MRMessageWordCount(MRJob):
    INPUT_PROTOCOL = JSONProtocol

    def mapper_init(self):
        self.vocabulary = {word.lower(): None for word in set(words.words())}
        self.words = Counter()

    def mapper(self, _, email):
        _, message = email
        words = (term for term in WORD.findall(message)
                 if term in self.vocabulary)

        self.words.update(words)

    def mapper_final(self):
        for word, occurences in self.words.items():
            yield word, occurences

    def reducer(self, word, occurences):
        count = sum(occurences)

        if count >= 10:
            yield word, count


if __name__ == "__main__":
    MRMessageWordCount.run()
