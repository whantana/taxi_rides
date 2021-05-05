import argparse
import sys

from util import log


def run(argv, parser):
    """

    :param argv:
    :param parser:
    :return:
    """
    args = setup_parser(parser).parse_args(argv)
    log("Hello dask")
    log(args)


def setup_parser(parser):
    """

    :param parser:
    :return:
    """
    from util import setup_parser
    parser = setup_parser(parser)
    return parser


if __name__ == "__main__":
    run(sys.argv[1:], argparse.ArgumentParser())
