import argparse
from util import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def run(argv, parser):
    """
    Implementation of pyspark-flavored taxi rides.
    :param argv: arguments
    :param parser: argument parser isntance
    """
    # parse arguments
    args = setup_parser(init_parser(parser)).parse_args(argv)
    # app id
    app_id = generate_app_id()
    # start spark session
    spark = SparkSession.builder.master(args.master) \
        .config("spark.jars", args.jars) \
        .config("spark.executor.cores", args.executor_cores) \
        .config("spark.driver.cores", args.driver_cores) \
        .config("spark.executor.memory", args.executor_memory) \
        .config("spark.driver.memory", args.driver_memory) \
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("fs.azure.account.auth.type", "OAuth") \
        .config("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
        .config("fs.azure.account.oauth2.client.id", args.azure_client_id) \
        .config("fs.azure.account.oauth2.client.secret", args.azure_client_secret) \
        .config("fs.azure.account.oauth2.client.endpoint",
                "https://login.microsoftonline.com/" + args.azure_tenant_id + "/oauth2/token") \
        .appName("taxi_rides_data_ingestion_pyspark_{}".format(app_id)).getOrCreate()
    # get taxi zones dataframe
    taxi_zones_df = taxi_zones(spark, args.taxi_zones_path)
    info("Taxi-zones dataframe loaded (csv-src={}).".format(args.taxi_zones_path))
    # get taxi rides dataframe
    taxi_rides_df = taxi_rides(spark, args.taxi_trips_path)
    info("Taxi-rides dataframe loaded (src={}).".format(args.taxi_trips_path))
    # get filtering_rules & pandas expression
    filtering_expression = filtering_rules_to_expression(get_filtering_rules(args), rule_to_expression)
    # filter
    if filtering_expression is not None:
        info("Applying filtering expression : {}".format(filtering_expression))
        taxi_rides_df.createOrReplaceTempView("taxi_rides")
        taxi_rides_df = spark.sql("SELECT * from taxi_rides WHERE {}".format(filtering_expression))
    # join
    info("Joining dataframes")
    taxi_rides_df = join_with_zones(taxi_rides_df, taxi_zones_df)
    # generate app_id and date_PU column
    info("Generating additional columns")
    taxi_rides_df = taxi_rides_df.withColumn("app_id", lit(app_id).cast(StringType()))
    taxi_rides_df = taxi_rides_df.withColumn("date_PU",
                                             from_unixtime(unix_timestamp(taxi_rides_df["datetime_PU"]), "yyyyMMdd"))
    # write
    taxi_rides_df.write.parquet(args.output_path, mode="append", partitionBy=["date_PU", "borough_PU"])
    info("Done. Data persisted at : {}".format(args.output_path))


def taxi_zones(spark, path):
    """
    Return taxi-zones spark dataframe.
    :param spark: spark instance
    :param path: input path
    :return: dataframe
    """
    zone_schema = StructType([
        StructField("OBJECTID", StringType()),
        StructField("Shape_Leng", DoubleType()),
        StructField("the_geom", StringType()),
        StructField("Shape_Area", DoubleType()),
        StructField("zone", StringType()),
        StructField("LocationID", IntegerType()),
        StructField("borough", StringType())
    ])
    zdf = spark.read.csv(path, header=True,
                         schema=zone_schema,
                         mode="FAILFAST")
    return zdf.drop("OBJECTID")


def taxi_rides(spark, path):
    """
    Return taxi-rides spark dataframe.
    :param spark: spark instance
    :param path: input path
    :return: dataframe
    """
    trip_schema = StructType([
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", IntegerType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("payment_type", IntegerType()),
        StructField("fare_amount", DecimalType(5, 2)),
        StructField("extra", DecimalType(4, 2)),
        StructField("mta_tax", DecimalType(2, 2)),
        StructField("tip_amount", DecimalType(5, 2)),
        StructField("tolls_amount", DecimalType(5, 2)),
        StructField("improvement_surcharge", DecimalType(2, 2)),
        StructField("total_amount", DecimalType(5, 2))
    ])
    timestamp_fmt = "MM/dd/yyyy hh:mm:ss aa"
    tdf = spark.read.csv(path, header=True,
                         schema=trip_schema,
                         timestampFormat=timestamp_fmt,
                         mode="FAILFAST")
    tdf = tdf.withColumnRenamed("tpep_pickup_datetime", "datetime_PU")
    tdf = tdf.withColumnRenamed("tpep_dropoff_datetime", "datetime_DO")
    return tdf.withColumn("store_and_fwd_flag", tdf["store_and_fwd_flag"].cast("boolean"))


def rule_to_expression(filtering_rule):
    """
    Single filtering rule to expression
    :param filtering_rule:
    :return: expression
    """
    column = filtering_rule["column"]
    filter_type = filtering_rule["type"]
    filter_params = filtering_rule["value"]
    if filter_type == "range":
        sdt = "to_timestamp(\"{}-{}-{}\",\"yyyy-MM-dd\")".format(filter_params[0][0], filter_params[0][1], filter_params[0][2])
        edt = "to_timestamp(\"{}-{}-{}\",\"yyyy-MM-dd\")".format(filter_params[1][0], filter_params[1][1], filter_params[1][2])
        return "{} >= {} and {} <= {}".format(column, sdt, column, edt)
    elif filter_type == "date":
        return "date_format({},\"yyyy-MM-dd\") == \"{}-{}-{}\"".format(column, filter_params[0],filter_params[1],filter_params[2])
    elif filter_type == "year-month":
        return "year({}) == {} and month({}) == {}".format(column, filter_params[0], column, filter_params[1])
    elif filter_type == "year":
        return "year({}) == {}".format(column, filter_params)


def join_with_zones(taxi_rides_df, taxi_zones_df):
    """
    Perform joining taxi-rides and taxi zones dataframes on Location ID.
    :param taxi_rides_df: taxi rides dataframe
    :param taxi_zones_df: taxi zone dataframe
    :return: joined  taxi-rides and taxi zones dataframes on Location ID
    """
    taxi_rides_df = taxi_rides_df.join(taxi_zones_df,
                                       taxi_rides_df["PULocationID"] == taxi_zones_df["LocationID"], "inner")
    taxi_rides_df = taxi_rides_df.drop("PULocationID", "LocationID")
    taxi_rides_df = taxi_rides_df.withColumnRenamed("Shape_Leng", "Shape_Leng_PU") \
        .withColumnRenamed("the_geom", "the_geom_PU") \
        .withColumnRenamed("Shape_Area", "Shape_Area_PU") \
        .withColumnRenamed("zone", "zone_PU") \
        .withColumnRenamed("borough", "borough_PU")
    taxi_rides_df = taxi_rides_df.join(taxi_zones_df,
                                       taxi_rides_df["DOLocationID"] == taxi_zones_df["LocationID"], "inner")
    taxi_rides_df = taxi_rides_df.drop("DOLocationID", "LocationID")
    taxi_rides_df = taxi_rides_df.withColumnRenamed("Shape_Leng", "Shape_Leng_DO") \
        .withColumnRenamed("the_geom", "the_geom_DO") \
        .withColumnRenamed("Shape_Area", "Shape_Area_DO") \
        .withColumnRenamed("zone", "zone_DO") \
        .withColumnRenamed("borough", "borough_DO")
    return taxi_rides_df


def setup_parser(parser):
    """
    Setup argument parser.
    :param parser: argument parser
    :return: configured parser
    """
    parser.add_argument('--master', required=False, default="local[*]",
                        help="Spark master URL defaults to 'local'.")
    parser.add_argument('--driver-cores', required=False, type=int, default="3",
                        help="Number of cores.")
    parser.add_argument('--driver-memory', required=False, default="8g",
                        help="Memory limit of Spark driver.")
    parser.add_argument('--executor-cores', required=False, type=int, default="4",
                        help="Number of cores for each executor.")
    parser.add_argument('--executor-memory', required=False, default="8g",
                        help="Memory limit of Spark executor.")
    parser.add_argument('--jars', required=False, default="",
                        help="Additional JARs to be included in driver and executors(comma-seperated).")
    return parser


if __name__ == "__main__":
    run(sys.argv[1:], argparse.ArgumentParser())
