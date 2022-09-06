# NormaliseMRJob.py

from mrjob.job import MRJob

class NormaliseJob(MRJob):

    minmaxdata = []

    DELIMITER = ","

    def configure_args(self):
        super(NormaliseJob, self).configure_args()
        self.add_file_arg("--minmax", type=str)

    def mapper_init(self):
        with open(self.options.minmax,"r") as f:
            for line in f:
                fieldvalues = line.split(self.DELIMITER)
                colnumber = int(fieldvalues[0])
                minimum = float(fieldvalues[1])
                maximum = float(fieldvalues[2])
                self.minmaxdata.append([colnumber,minimum,maximum])

    def mapper(self, _, line):
        minmaxdata = self.minmaxdata
        field_values = line.strip().split(self.DELIMITER)
        col = 0
        out_values = []
        for value in field_values:
            norm_values = [x for x in minmaxdata if x[0] == col]
            col += 1
            if norm_values:
                minimum = norm_values[0][1]
                maximum = norm_values[0][2]
                out_values.append((float(value) - minimum) / (minimum - maximum))
            else:
                out_values.append(value)
        yield out_values, None



if __name__ == '__main__':
    NormaliseJob.run()