from mrjob.job import MRJob


class MRWordCountInMapperCombiner(MRJob):
    def mapper_init(self):
        self.terms = {}

    def mapper(self, _, line):
        for term in line.split():
            if term not in self.terms:
                self.terms[term] = 0

            self.terms[term] += 1

    def mapper_final(self):
        for term, occurences in self.terms.items():
            yield term, occurences

    def reducer(self, term, occurences):
        yield term, sum(occurences)


if __name__ == '__main__':
    MRWordCountInMapperCombiner.run()
