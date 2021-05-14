import sys
import os
import argparse

# Modes available  ( maybe pandas-dask-yarn)
from util import error

ALL_MODES = ["shell", "pandas", "dask", "pyspark"]


def main(argv):
    """
    Main function
    :param argv: arguments
    :return: exit code
    """

    mode = argv[0]
    args = argv[1:]
    if mode == "shell":
        from taxi_rides_data_ingestion_shell import run
    elif mode == "pandas":
        from taxi_rides_data_ingestion_pandas import run
    elif mode == "dask":
        from taxi_rides_data_ingestion_dask import run
    elif mode == "pyspark":
        from taxi_rides_data_ingestion_pyspark import run
    else:
        error("Wrong mode \"{}\". Please provide mode from the available ones {}".format(mode, str(ALL_MODES)))
        exit(2)
    run(args, argparse.ArgumentParser(prog="{} {}".format(os.path.basename(sys.argv[0]), mode),
                                      description="Taxi zone data ingestion (mode={})".format(mode)))


if __name__ == "__main__":
    main(sys.argv[1:])
