from predict_receiver import MRPredictReceiver


class CSV(object):
    def write(self, key, value):
        return "{}, {}".format(key, ", ".join(value)).encode("utf-8")


class MRPredictReceiverCSV(MRPredictReceiver):
    OUTPUT_PROTOCOL = CSV


if __name__ == '__main__':
    MRPredictReceiverCSV.run()
