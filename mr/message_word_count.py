import re
from collections import defaultdict

from mrjob.job import MRJob
from nltk.corpus import words

MESSAGE_ID = "\",\"Message-ID: "
DELIMITERS = "[^A-Za-z\-]"


class MRMessageWordCount(MRJob):
    def mapper_init(self):
        self.vocabulary = {word: 0 for word in set(words.words())}
        self.words = defaultdict(int)

        self.buffer_lines = False
        self.lines = []

    def mapper(self, _, line):
        line = line.strip()

        if MESSAGE_ID in line:
            self.buffer_lines = False
        elif not line:
            self.buffer_lines = True

        if self.buffer_lines:
            self.lines.append(line)
        else:
            message = "".join(self.lines)
            terms = (term for term in re.split(DELIMITERS, message)
                     if len(term) and term in self.vocabulary)

            for term in terms:
                self.words[term] += 1

            self.lines = []

    def mapper_final(self):
        for word, occurences in self.words.items():
            yield word, occurences

    def reducer(self, word, count):
        count = sum(count)
        if count >= 10:
            yield word, count


if __name__ == '__main__':
    MRMessageWordCount.run()
