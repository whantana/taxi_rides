import argparse
import decimal
import sys
import pandas as pd

from util import log, get_filtering_rules, generate_app_id , generate_storage_options


def run(argv, parser):
    """

    :param argv:
    :param parser:
    :return:
    """

    # parse arguments
    args = setup_parser(parser).parse_args(argv)

    # get filtering_rules & pandas expression
    filtering_rules = get_filtering_rules(args)
    filtering_expression = filtering_rules_to_expression(filtering_rules)

    # output path
    output_path = args.output_path

    # set storage options if provided
    storage_options = generate_storage_options(args)

    # get taxi zones dataframe
    taxi_zones_path = args.taxi_zones_path
    taxi_zones_df = taxi_zones(taxi_zones_path,storage_options)
    log("Taxi-zones dataframe loaded (csv-src={}).".format(taxi_zones_path))

    # app id
    app_id = generate_app_id()

    # taxi rides path
    taxi_trips_path = args.taxi_trips_path

    # processing taxi rides input, if chunking is enabled iterative approach is run
    if args.chunksize is not None:
        chunk_size = args.chunksize
        chunk_iterator = taxi_rides_iterator(taxi_trips_path, chunk_size, storage_options)
        for i, taxi_rides_df in enumerate(chunk_iterator, 1):
            log("Taxi-rides dataframe iterator (src={}, chunk-size={}, chunk={})."
                .format(taxi_trips_path, chunk_size, i))
            # filter
            if filtering_expression is not None:
                taxi_rides_df = taxi_rides_df.query(filtering_expression)
            # join
            taxi_rides_df = join_with_zones(taxi_rides_df, taxi_zones_df)

            # generate app_id and date_PU column
            taxi_rides_df["app_id"] = app_id
            taxi_rides_df["date_PU"] = taxi_rides_df["datetime_PU"].dt.strftime("%Y%m%d")

            # write
            taxi_rides_df.to_parquet(output_path, partition_cols=["date_PU", "borough_PU"],
                                     storage_options= storage_options,
                                     compression=None)
            log("Done( chunk={})".format(i))
    else:
        if args.samplesize is not None:
            # open sample and filter by dates
            sample_size = args.samplesize
            taxi_rides_df = taxi_rides_sample(taxi_trips_path, sample_size , storage_options)
            log("Taxi-rides dataframe sample (src={}, sample-size={}).".format(taxi_trips_path, sample_size))
        else:
            # open whole dataset and filter by dates
            taxi_rides_df = taxi_rides(taxi_trips_path)
            log("Taxi-rides dataframe loaded (src={}).".format(taxi_trips_path))
        # filter
        if filtering_expression is not None:
            taxi_rides_df = taxi_rides_df.query(filtering_expression)
        # join
        taxi_rides_df = join_with_zones(taxi_rides_df, taxi_zones_df)

        # generate app_id and date_PU column
        taxi_rides_df["app_id"] = app_id
        taxi_rides_df["date_PU"] = taxi_rides_df["datetime_PU"].dt.strftime("%Y%m%d")

        # write
        taxi_rides_df.to_parquet(output_path, partition_cols=["date_PU", "borough_PU"],
                                 storage_options=storage_options, compression=None)
        log("Done. Data persisted at : {}".format(output_path))


def taxi_zones(path, storage_options=None):
    """

    :param storage_options:
    :param path:
    :return:
    """

    zdf = pd.read_csv(path, storage_options=storage_options)
    zdf.drop("OBJECTID", axis="columns", inplace=True)
    zdf.set_index("LocationID", inplace=True)
    return zdf


def taxi_rides(path, storage_options=None):
    """

    :param storage_options:
    :param path:
    :return:
    """

    # transformation function for proper datetime type
    date_parser = lambda col: pd.to_datetime(col, format="%m/%d/%Y %I:%M:%S %p")
    # transformation function for proper monetary columns
    to_decimal = lambda x: decimal.Decimal("{:.2f}".format(round(float(x), 2)))
    monetary_columns = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                        "total_amount"]
    decimal_converters = dict(((column, to_decimal) for column in monetary_columns))
    # get dataframe
    trdf = pd.read_csv(path, parse_dates={"datetime_PU": ["tpep_pickup_datetime"],
                                          "datetime_DO": ["tpep_dropoff_datetime"]},
                       date_parser=date_parser,
                       converters=decimal_converters,
                       true_values='Y', false_values='N',
                       storage_options=storage_options)
    return trdf


def taxi_rides_iterator(path, n, storage_options=None):
    """

    :param storage_options:
    :param path:
    :param n:
    :return:
    """

    # transformation function for proper datetime type
    date_parser = lambda col: pd.to_datetime(col, format="%m/%d/%Y %I:%M:%S %p")

    # transformation function for proper monetary columns
    to_decimal = lambda x: decimal.Decimal("{:.2f}".format(round(float(x), 2)))
    monetary_columns = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                        "total_amount"]
    decimal_converters = dict(((column, to_decimal) for column in monetary_columns))
    # get reader iterator for chunksize n
    csv_iter = pd.read_csv(path, chunksize=n,
                           parse_dates={"datetime_PU": ["tpep_pickup_datetime"],
                                        "datetime_DO": ["tpep_dropoff_datetime"]},
                           date_parser=date_parser,
                           converters=decimal_converters,
                           true_values='Y', false_values='N',
                           storage_options=storage_options)
    return csv_iter


def taxi_rides_sample(path, n, storage_options=None):
    """

    :param path:
    :param n:
    :param storage_options:
    :return:
    """
    return next(taxi_rides_iterator(path, n, storage_options))


def filtering_rules_to_expression(filtering_rules):
    """

    :param filtering_rules:
    :return:
    """

    def rule_to_expression(column, filter_type, filter_params):
        if filter_type == "range":
            sdt = "Timestamp({},{},{})".format(filter_params[0][0], filter_params[0][1], filter_params[0][2])
            edt = "Timestamp({},{},{})".format(filter_params[1][0], filter_params[1][1], filter_params[1][2])
            return "{}.dt.date >= Timestamp({}) and {}.dt.date <= Timestamp({})".format(column, sdt, column, edt)
        elif filter_type == "date":
            return "{}.dt.year == {} and {}.dt.month == {} and {}.dt.day == {}".format(column, filter_params[0],
                                                                                       filter_params[1],
                                                                                       filter_params[2])
        elif filter_type == "year-month":
            return "{}.dt.year == {} and {}.dt.month == {}".format(column, filter_params[0], filter_params[1])
        elif filter_type == "year":
            return "{}.dt.year == {}".format(column, filter_params)

    total_rules = 0

    for (column, filter_name) in [("datetime_PU", "filter_PU"), ("datetime_DO", "filter_DO")]:
        if filtering_rules[filter_name] is not None:
            total_rules = total_rules + 1

    if total_rules == 0:
        return None
    elif total_rules == 1:
        for (column, filter_name) in [("datetime_PU", "filter_PU"), ("datetime_DO", "filter_DO")]:
            if filtering_rules[filter_name] is not None:
                filter_type = filtering_rules[filter_name][0]
                filter_params = filtering_rules[filter_name][1]
                return rule_to_expression(column, filter_type, filter_params)
    elif total_rules == 2:
        return rule_to_expression("datetime_PU", filtering_rules["filter_PU"][0],
                                  filtering_rules["filter_PU"][1]) + " and " + \
               rule_to_expression("datetime_DO", filtering_rules["filter_DO"][0], filtering_rules["filter_DO"][1])


def join_with_zones(taxi_rides_df, taxi_zones_df):
    """

    :param taxi_rides_df:
    :param taxi_zones_df:
    :return:
    """
    taxi_rides_df = taxi_rides_df.merge(taxi_zones_df, how="inner", sort=False,
                                        left_on="PULocationID",
                                        right_on="LocationID",
                                        right_index=True)
    taxi_rides_df.drop("PULocationID", axis="columns", inplace=True)
    taxi_rides_df = taxi_rides_df.merge(taxi_zones_df, how="inner", sort=False,
                                        left_on="DOLocationID",
                                        right_on="LocationID",
                                        suffixes=["_PU", "_DO"],
                                        right_index=True)
    taxi_rides_df.drop("DOLocationID", axis="columns", inplace=True)
    return taxi_rides_df


def setup_parser(parser):
    """

    :param parser:
    :return:
    """
    from util import setup_parser
    parser = setup_parser(parser)
    parser.add_argument('--chunksize', type=int, required=False,
                        help="Use chunking for large files. N as size of chunk.")
    parser.add_argument('--samplesize', type=int, required=False, help="Use sampling. N as first rows read")
    return parser


if __name__ == "__main__":
    run(sys.argv[1:], argparse.ArgumentParser())
