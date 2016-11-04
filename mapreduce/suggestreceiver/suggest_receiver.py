from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol

from predict_receiver import MRPredictReceiver


class MRSuggestReceiver(MRPredictReceiver):
    def configure_options(self):
        super(MRSuggestReceiver, self).configure_options()

        self.add_passthrough_option(
            "--join-only",
            action="store_true",
            dest="join_only",
            help="Only perform the join steps on a preprocessed dataset.")

    def input_protocol(self):
        if self.options.join_only:
            return JSONProtocol()

        return super(MRSuggestReceiver, self).input_protocol()

    def steps(self):
        if self.options.join_only:
            return [
                MRStep(mapper=self.mapper_join,
                       reducer=self.reducer_join),
                MRStep(mapper=self.mapper_reassemble,
                       reducer=self.reducer_reassemble)
            ]

        return super(MRSuggestReceiver, self).steps() + [
            MRStep(mapper=self.mapper_join,
                   reducer=self.reducer_join),
            MRStep(mapper=self.mapper_reassemble,
                   reducer=self.reducer_reassemble)
        ]

    def mapper_join(self, sender, receivers):
        receivers = list(receivers)
        yield sender, receivers
        for receiver in receivers:
            yield receiver, [sender]

    def reducer_join(self, email, links):
        links = list(links)
        metadata = max(enumerate(links), key=lambda tup: len(tup[1]))
        email_recv_pred = metadata[1]
        # Only yield if there is enough emails to form a top 3 list
        if len(email_recv_pred) == 3:
            del links[metadata[0]]
            for link in links:
                yield link[0], email_recv_pred

    def mapper_reassemble(self, sender, suggestions):
        suggestions = list(set(suggestions))
        if sender in suggestions:
            suggestions.remove(sender)
        yield sender, suggestions

    def reducer_reassemble(self, sender, suggestions):
        yield sender, list(set(sum(suggestions, [])))


if __name__ == "__main__":
    MRSuggestReceiver.run()
