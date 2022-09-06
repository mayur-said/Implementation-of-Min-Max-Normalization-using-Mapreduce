#!/usr/bin/env python
# normalise.py

import sys
from MinMaxMRJob import MinMaxJob
from NormaliseMRJob import NormaliseJob

coldata = []

if __name__ == '__main__':
    args = sys.argv[1:]
    minmaxjob = MinMaxJob(args)
    with minmaxjob.make_runner() as runner:
        runner.run()
        with open("minmax.txt", "w") as f:
            for column, value in minmaxjob.parse_output(runner.cat_output()):
                f.write(",".join(str(val) for val in [column]+value)+"\n")
    args = ['--minmax','minmax.txt'] + args
    normalisejob = NormaliseJob(args)
    with normalisejob.make_runner() as runner:
        runner.run()
        for key, _ in normalisejob.parse_output(runner.cat_output()):
            print(key)