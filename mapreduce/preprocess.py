"""
MRPreprocess preprocesses emails.csv into the format (id, (headers, message)).
This is done for both performance and clarity of code. The headers are
separated by the constant SEPERATOR so that they might be reconstucted at a
later time, i.e. for being used by a library that expects the headers to be in
their original format. In the body of the email new lines are replaced by
spaces, as we o not care about the structure.

If the output of this job is to be correct, we must ensure that the input data
is not split. i.e one mapper receives the whole dataset. This can be done by
setting the mapreduce.input.fileinputformat.split.minsize to the size of the
dataset.
"""

from mrjob.job import MRJob

MESSAGE_ID = "\",\"Message-ID: "
RECORD_DELIMITER = "\30"  # ASCII record separator


class MRPreprocess(MRJob):
    def mapper_init(self):
        self.buffer_lines = False
        self.id = ""
        self.headers = []
        self.lines = []

    def mapper(self, _, line):
        if MESSAGE_ID in line:
            if self.id:
                headers = RECORD_DELIMITER.join(self.headers).lower()
                message = " ".join(self.lines)

                yield self.id, (headers, message)

                self.headers = []
                self.lines = []

            self.id = line[line.find(MESSAGE_ID) + len(MESSAGE_ID) + 1:-1]
            self.buffer_lines = False
        elif not line and not self.buffer_lines:
            self.buffer_lines = True
        elif self.buffer_lines:
            self.lines.append(line)
        elif self.id:
            self.headers.append(line)

    def mapper_final(self):
        if self.id:
            headers = " ".join(self.headers).lower()
            message = " ".join(self.lines).lower()

            yield self.id, (headers, message)


if __name__ == "__main__":
    MRPreprocess.run()
