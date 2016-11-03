from mrjob.job import MRJob


class MRCount(MRJob):
    def configure_options(self):
        super(MRCount, self).configure_options()

        self.add_passthrough_option(
            "--total-only",
            action="store_true",
            dest="total_only",
            help=("Outputs the total number of words encountered"
                  " including duplicates"))

    def getKey(self, key):
        if (self.options.total_only):
            return "Total"

        return key
