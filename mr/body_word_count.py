from mrjob.job import MRJob
from datetime import datetime
from nltk.corpus import words

class MRBody_word_count(MRJob):

    def mapper_init(self):
        self.in_body = False
        self.body = []
        self.wordset = set(words.words())


    def mapper(self, _, line):
        line = line.strip()

        if not line and not self.in_body:
            self.in_body = True
        elif ',"Message-ID: ' in line:
            self.in_body = False

            # add some form of filtering
            words = [word.strip("<> \\\"\'") for word in "".join(self.body).split(" ,.")
                     if word.strip("<> \\\"\'") in self.wordset]

            self.body = []
            count = {}

            for word in words:
                if word in count:
                    count[word] += 1
                else:
                    count[word] = 1

            for k, v in count.items():
                yield k, v

        if self.in_body:
            self.body.append(line)


    def combiner(self, word, count):
        count = sum(count)
        if count >= 10:
            yield word, count


    def reducer(self, word, count):
        count = sum(count)
        if count >= 10:
            yield word, count

if __name__ == '__main__':
    MRBody_word_count.run()