from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
from predict_receiver import MRPredictReceiver


class MRPredictReceiverCSV(MRPredictReceiver):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_to_from_init,
                   mapper=self.mapper_from_to,
                   mapper_final=self.mapper_to_from_final,
                   reducer=self.reducer_from_to),
            MRStep(mapper=self.mapper_predict,
                   reducer=self.reducer_predict),
            MRStep(mapper=self.to_csv)
        ]

    def to_csv(self, key, values):
        values = list(values)
        yield "", "{}, ".format(key) + ", ".join(values)

if __name__ == '__main__':
    MRPredictReceiverCSV.run()