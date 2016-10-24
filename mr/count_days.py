from mrjob.job import MRJob

class MRCountDays(MRJob):

    def mapper(self, _, line):
        if line.startswith("Date: "):
            day = line[6:9]
            if day.lower() in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']:
                yield day.lower(), 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRCountDays.run()