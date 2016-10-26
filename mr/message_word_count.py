import re

from mrjob.job import MRJob
from nltk.corpus import words

MESSAGE_ID = "\",\"Message-ID: "
FILTER = "<> \\\"\'"
DELIMITERS = "\.|,| "


class MRMessageWordCount(MRJob):
    def mapper_init(self):
        self.vocabulary = {word: None for word in set(words.words())}
        self.words = {}

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

            # Clean up terms.
            terms = [term.strip(FILTER)
                     for term in re.split(DELIMITERS, message)]

            for term in terms:
                if term not in self.vocabulary:
                    continue

                if term not in self.words:
                    self.words[term] = 0

                self.words[term] += 1

            self.lines = []

    def mapper_final(self):
        for word in self.words:
            yield word, self.words[word]

    def reducer(self, word, count):
        count = sum(count)
        if count >= 10:
            yield word, count


if __name__ == '__main__':
    MRMessageWordCount.run()
