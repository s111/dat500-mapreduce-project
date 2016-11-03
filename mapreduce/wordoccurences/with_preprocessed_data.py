import re
from collections import Counter

from mrjob.protocol import JSONProtocol
from nltk.corpus import words

from MRCount import MRCount

WORD = re.compile("\w+")


class MRMessageWordCount(MRCount):
    INPUT_PROTOCOL = JSONProtocol

    def mapper_init(self):
        self.vocabulary = set(map(str.lower, words.words()))
        self.words = Counter()

    def mapper(self, _, email):
        _, message = email
        words = (term for term in WORD.findall(message.lower())
                 if term in self.vocabulary)

        self.words.update(words)

    def mapper_final(self):
        for word, occurences in self.words.items():
            yield self.getKey(word), occurences

    def reducer(self, word, occurences):
        yield word, sum(occurences)


if __name__ == "__main__":
    MRMessageWordCount.run()
