class CSVOutputProtocol(object):
    def write(self, sender, receivers):
        return "{}, {}".format(sender, ", ".join(receivers)).encode("utf-8")
