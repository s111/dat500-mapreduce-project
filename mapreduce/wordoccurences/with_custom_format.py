import re
from collections import Counter

from mrjob.protocol import RawProtocol
from nltk.corpus import words

from MRCount import MRCount

RECORD_DELIMITER = "\30"
WORD = re.compile("\w+")


class MRMessageWordCount(MRCount):
    HADOOP_INPUT_FORMAT = ("com.sebastianpedersen"
                           ".hadoop.mapred.MessageInputFormat")
    INPUT_PROTOCOL = RawProtocol

    def mapper_init(self):
        self.vocabulary = set(map(str.lower, words.words()))
        self.words = Counter()

    def mapper(self, _, email):
        # Discard header.
        start = email.find(2 * RECORD_DELIMITER)
        # Discard last line.
        end = email.rfind(RECORD_DELIMITER)

        words = (term for term in WORD.findall(email[start:end].lower())
                 if term in self.vocabulary)

        self.words.update(words)

    def mapper_final(self):
        for word, occurences in self.words.items():
            yield self.getKey(word), occurences

    def reducer(self, word, occurences):
        yield word, sum(occurences)


if __name__ == "__main__":
    MRMessageWordCount.run()
