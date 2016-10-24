from mrjob.job import MRJob

class MRCountDays(MRJob):

    def mapper(self, _, line):
        #line = line.strip() # remove leading and trailing whitespace
        if line.startswith("Date: "):
            #idx = 6  # line.startswith("Date: ")+1
            day = line[6:9]
            if day.lower() in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']:
                yield day.lower(), 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRCountDays.run()