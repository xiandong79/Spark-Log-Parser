import os
import sys

from spark_run import SparkRun

def parse_dir(path):
    for dirpath, subdirs, files in os.walk(path):
        for file in files:
            try:
                a = SparkRun(os.path.join(dirpath, files))
            except ValueError
                continue
            a.correlate()
            # Link block managers and executors
            report = a.generate_report()
            open(os.path.join(dirpath, a.get_app_name() + ".txt"), "w").write(report)


if __name__ = "__main__":
    if len(sys.argv) < 1:
        print "Usage python <main.py> <log_file>"
    else:
        PATH = os.path.join(sys.argv[1])

    parse_dir(PATH)
