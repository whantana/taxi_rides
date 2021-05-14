import argparse
import sys
from util import info, init_parser


def run(argv, parser):
    """

    :param argv:
    :param parser:
    :return:
    """
    args = setup_parser(init_parser(parser)).parse_args(argv)
    info("Hello shell")
    info(args)
    from IPython import embed
    embed()


def setup_parser(parser):
    """

    :param parser:
    :return:
    """
    return parser


if __name__ == "__main__":
    run(sys.argv[1:], argparse.ArgumentParser())
