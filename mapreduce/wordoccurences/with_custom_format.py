import re
from collections import Counter

from mrjob.job import MRJob
from mrjob.protocol import RawProtocol
from nltk.corpus import words

RECORD_DELIMITER = "\30"
WORD = re.compile("\w+")


class MRMessageWordCount(MRJob):
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
            yield word, occurences

    def reducer(self, word, occurences):
        count = sum(occurences)

        if count >= 10:
            yield word, count


if __name__ == "__main__":
    MRMessageWordCount.run()
