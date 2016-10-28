import re
from collections import defaultdict

from mrjob.job import MRJob
from nltk.corpus import words

MESSAGE_ID = "\",\"Message-ID: "
WORD = re.compile("\w+")


class MRMessageWordCount(MRJob):
    def mapper_init(self):
        self.vocabulary = {word.lower(): 0 for word in set(words.words())}
        self.words = defaultdict(int)

        self.buffer_lines = False
        self.lines = []

    def mapper(self, _, line):
        if MESSAGE_ID in line:
            self.buffer_lines = False

            message = "".join(self.lines).lower()
            words = (term for term in WORD.findall(message)
                     if term in self.vocabulary)

            for word in words:
                self.words[word] += 1

            self.lines = []
        elif not line:
            self.buffer_lines = True

        if self.buffer_lines:
            self.lines.append(line)

    def mapper_final(self):
        for word, occurences in self.words.items():
            yield word, occurences

    def reducer(self, word, occurences):
        count = sum(occurences)

        if count >= 10:
            yield word, count


if __name__ == "__main__":
    MRMessageWordCount.run()
