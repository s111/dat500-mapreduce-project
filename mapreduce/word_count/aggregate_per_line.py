from mrjob.job import MRJob


class MRWordCountAggregatePerLine(MRJob):
    def mapper(self, _, line):
        terms = {}

        for term in line.split():
            if term not in terms:
                terms[term] = 0

            terms[term] += 1

        for term, occurences in terms.items():
            yield term, occurences

    def reducer(self, term, occurences):
        yield term, sum(occurences)


if __name__ == '__main__':
    MRWordCountAggregatePerLine.run()
