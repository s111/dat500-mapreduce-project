from mrjob.job import MRJob


class MRWordCountWithCombiner(MRJob):
    def mapper(self, _, line):
        for term in line.split():
            yield term, 1

    def combiner(self, term, occurences):
        yield term, sum(occurences)

    def reducer(self, term, occurences):
        yield term, sum(occurences)


if __name__ == '__main__':
    MRWordCountWithCombiner.run()
