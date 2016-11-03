import re
from collections import Counter

from nltk.corpus import words

from MRCount import MRCount

MESSAGE_ID = "\",\"Message-ID: "
WORD = re.compile("\w+")


class MRMessageWordCount(MRCount):
    def mapper_init(self):
        self.vocabulary = set(map(str.lower, words.words()))
        self.words = Counter()

        self.buffer_lines = False
        self.skip = True
        self.lines = []

    def updateWords(self):
        message = " ".join(self.lines).lower()
        words = (term for term in WORD.findall(message))

        self.words.update(words)

    def mapper(self, _, line):
        if MESSAGE_ID in line:
            if not self.skip:
                self.buffer_lines = False
                self.updateWords()
                self.lines = []
            else:
                self.skip = False
        elif not line and not self.skip:
            self.buffer_lines = True
        elif self.buffer_lines:
            self.lines.append(line)

    def mapper_final(self):
        self.updateWords()

        valid = set(self.words.keys()).intersection(self.vocabulary)

        for word in valid:
            yield self.getKey(word), self.words[word]

    def reducer(self, word, occurences):
        yield word, sum(occurences)


if __name__ == "__main__":
    MRMessageWordCount.run()
