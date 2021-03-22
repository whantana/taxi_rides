import sys
import os
import argparse
from util import error

# Modes available  ( maybe pandas-dask-yarn)
ALL_MODES = ["shell", "pandas", "dask", "pyspark"]


def main(argv):
    """

    :param argv:
    :return:
    """

    mode = argv[0]
    args = argv[1:]
    prog = "{} {}".format(os.path.basename(sys.argv[0]), mode)
    if mode == "shell":
        from taxi_rides_ingestion_shell import run
    elif mode == "pandas":
        from taxi_rides_ingestion_pandas import run
    elif mode == "dask":
        from taxi_rides_ingestion_pandas_dask import run
    elif mode == "pyspark":
        from taxi_rides_ingestion_pandas_pyspark import run
    else:
        error("Wrong mode \"{}\". Please provide mode from the available ones {}".format(mode, str(ALL_MODES)))
        exit(2)
    run(args, argparse.ArgumentParser(prog=prog, description="Taxi zone data ingestion (mode={})".format(mode)))


if __name__ == "__main__":
    main(sys.argv[1:])
