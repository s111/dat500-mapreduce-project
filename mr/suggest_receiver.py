from mrjob.step import MRStep

from predict_receiver import MRPredictReceiver


class MRSuggestReceiver(MRPredictReceiver):
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_to_from_init,
                   mapper=self.mapper_from_to,
                   mapper_final=self.mapper_to_from_final,
                   reducer=self.reducer_from_to),
            MRStep(mapper=self.mapper_predict,
                   reducer=self.reducer_predict),
            MRStep(mapper=self.mapper_join,
                   reducer=self.reducer_join),
            MRStep(mapper=self.mapper_reassemble,
                   reducer=self.reducer_reassemble)
        ]

    def mapper_join(self, sender, receivers):
        receivers = list(receivers)
        yield sender, receivers
        for recv in receivers:
            yield recv, [sender]

    def reducer_join(self, email, links):
        links = list(links)
        metadata = max(enumerate(links), key=lambda tup: len(tup[1]))
        email_recv_pred = metadata[1]
        # If there is not enough mail sent by email to have a top 3 list from
        # second step, abort.
        if len(email_recv_pred) == 3:
            del links[metadata[0]]
            for link in links:
                yield link[0], email_recv_pred

    def mapper_reassemble(self, sender, suggestions):
        suggestions = list(suggestions)
        if sender in suggestions:
            suggestions.remove(sender)
        yield sender, suggestions

    def reducer_reassemble(self, sender, suggestions):
        tmp = []
        for l in list(suggestions):
            tmp += l
        yield sender, list(set(tmp))


if __name__ == "__main__":
    MRSuggestReceiver.run()
