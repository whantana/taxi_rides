import argparse
import decimal
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from util import *


def run(argv, parser):
    """
    Implementation of dask-flavored taxi rides.
    :param argv: arguments
    :param parser: argument parser isntance
    """
    # parse arguments
    args = setup_parser(init_parser(parser)).parse_args(argv)
    # set storage options if provided
    storage_options = generate_storage_options(args)
    # app id
    app_id = generate_app_id()
    # launch local cluster or register to remote one
    if args.scheduler is None or args.scheduler == "local":
        client = Client(LocalCluster(name="taxi_rides_dask_cluster",
                                     n_workers=args.local_n_workers,
                                     threads_per_worker=args.local_threads_per_worker))
    else:
        client = Client(args.scheduler)
    info("Dask cluster : {}.".format(client.cluster))
    # get taxi zones dataframe
    taxi_zones_df = taxi_zones(client, args.taxi_zones_path, storage_options)
    info("Taxi-zones dataframe loaded (csv-src={}).".format(args.taxi_zones_path))
    # get taxi rides dataframe
    taxi_rides_df = taxi_rides(args.taxi_trips_path, storage_options)
    info("Taxi-rides dataframe loaded (src={}).".format(args.taxi_trips_path))
    # get filtering_rules & pandas expression
    filtering_expression = filtering_rules_to_expression(get_filtering_rules(args), rule_to_expression)
    # filter
    if filtering_expression is not None:
        info("Applying filtering expression : {}".format(filtering_expression))
        taxi_rides_df = taxi_rides_df.query(filtering_expression)
    # join
    info("Joining dataframes")
    taxi_rides_df = join_with_zones(taxi_rides_df, taxi_zones_df)
    # generate app_id and date_PU column
    info("Generating additional columns")
    taxi_rides_df["app_id"] = app_id
    taxi_rides_df["date_PU"] = taxi_rides_df["datetime_PU"].dt.strftime("%Y%m%d")
    # write
    taxi_rides_df.to_parquet(args.output_path, partition_on=["date_PU", "borough_PU"],
                             storage_options=storage_options,
                             compression="snappy",
                             write_index=False)
    info("Done. Data persisted at : {}".format(args.output_path))


def taxi_zones(client, path, storage_options=None):
    """
    Return taxi-zones dask dataframe.
    :param client: client instance
    :param path: input path
    :param storage_options: storage options dict
    :return: taxi-zones dataframe
    """
    zdf = dd.read_csv(path, storage_options=storage_options)
    zdf = zdf.drop("OBJECTID", axis="columns")
    zdf = zdf.set_index("LocationID")
    return client.persist(zdf)


def taxi_rides(path, storage_options=None):
    """
    Return taxi-rides dask dataframe.
    :param path: input path
    :param storage_options: storage options dict
    :return: taxi-rides dataframe
    """
    # transformation function for proper datetime type
    date_parser = lambda col: pd.to_datetime(col, format="%m/%d/%Y %I:%M:%S %p")
    # transformation function for proper monetary columns
    to_decimal = lambda x: decimal.Decimal("{:.2f}".format(round(float(x), 2)))
    monetary_columns = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                        "total_amount"]
    decimal_converters = dict(((column, to_decimal) for column in monetary_columns))
    # get dataframe
    return dd.read_csv(path, parse_dates={"datetime_PU": ["tpep_pickup_datetime"],
                                          "datetime_DO": ["tpep_dropoff_datetime"]},
                       date_parser=date_parser,
                       converters=decimal_converters,
                       true_values='Y', false_values='N',
                       storage_options=storage_options)


def join_with_zones(taxi_rides_df, taxi_zones_df):
    """
    Perform joining taxi-rides and taxi zones dataframes on Location ID.
    :param taxi_rides_df: taxi rides dataframe
    :param taxi_zones_df: taxi zone dataframe
    :return: joined  taxi-rides and taxi zones dataframes on Location ID
    """
    # inner join on pickup location id, drop duplicate columns
    taxi_rides_df = taxi_rides_df.merge(taxi_zones_df, how="inner",
                                        left_on="PULocationID",
                                        right_on="LocationID",
                                        right_index=True)
    taxi_rides_df = taxi_rides_df.drop("PULocationID", axis="columns")
    # inner join on drop off location id, drop duplicate columns
    taxi_rides_df = taxi_rides_df.merge(taxi_zones_df, how="inner",
                                        left_on="DOLocationID",
                                        right_on="LocationID",
                                        suffixes=["_PU", "_DO"],
                                        right_index=True)
    taxi_rides_df = taxi_rides_df.drop("DOLocationID", axis="columns")
    return taxi_rides_df


# define rule to expression
def rule_to_expression(filtering_rule):
    """
        Single filtering rule to expression
        :param filtering_rule:
        :return:
        """
    column = filtering_rule["column"]
    filter_type = filtering_rule["type"]
    filter_params = filtering_rule["value"]
    if filter_type == "range":
        sdt = "Timestamp({},{},{})".format(filter_params[0][0], filter_params[0][1], filter_params[0][2])
        edt = "Timestamp({},{},{})".format(filter_params[1][0], filter_params[1][1], filter_params[1][2])
        return "{}.dt.date >= {} and {}.dt.date <= {}".format(column, sdt, column, edt)
    elif filter_type == "date":
        return "{}.dt.year == {} and {}.dt.month == {} and {}.dt.day == {}".format(column, filter_params[0],
                                                                                   column, filter_params[1],
                                                                                   column, filter_params[2])
    elif filter_type == "year-month":
        return "{}.dt.year == {} and {}.dt.month == {}".format(column, filter_params[0], column, filter_params[1])
    elif filter_type == "year":
        return "{}.dt.year == {}".format(column, filter_params)


def setup_parser(parser):
    """
    Setup argument parser.
    :param parser: argument parser
    :return: configured parser
    """
    parser.add_argument('--scheduler', required=False, default="local",
                        help="Dask scheduler ip:port for remote scheduler,'local' "
                             "or empty  for creating default local cluster.")
    parser.add_argument('--local-n-workers', required=False, type=int, default=2,
                        help="Number of Dask workers.")
    parser.add_argument('--local-threads-per-worker', required=False, type=int, default=6,
                        help="Number of threads per Dask worker.")
    parser.add_argument('--local-memory-limit', required=False, default="2G",
                        help="Memory limit of Dask worker.")

    return parser


if __name__ == "__main__":
    run(sys.argv[1:], argparse.ArgumentParser())
