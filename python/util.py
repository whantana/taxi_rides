import datetime
import secrets
import sys
import re


def log(msg):
    """

    :param msg:
    :return:
    """
    print("{} - {} ".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))


def error(msg):
    """

    :param msg:
    :return:
    """
    print(msg, file=sys.stderr)


def setup_parser(parser):
    """

    :param parser:
    :return:
    """
    parser.add_argument('--taxi-trips-path', required=True, help="Taxi trips input path.")
    parser.add_argument('--taxi-zones-path', required=True, help="Taxi zones input path.")
    parser.add_argument('--output-path', required=True, help="Output input path.")
    parser.add_argument('--filter-pickup', required=False, help="Filter by pickup date. Available formats :"
                                                                "[YYYY, YYYY-MM, YYYY-MM-DD, YYYY-MM-DD:YYYY-MM-DD]")
    parser.add_argument('--filter-dropoff', required=False, help="Filter by dropoff date. Available formats :"
                                                                 "[YYYY, YYYY-MM, YYYY-MM-DD, YYYY-MM-DD:YYYY-MM-DD]")
    parser.add_argument('--azure-tenant-id', required=False, help="Azure Tenant id")
    parser.add_argument('--azure-client-id', required=False, help="Azure Client id")
    parser.add_argument('--azure-client-secret', required=False, help="Azure Client Secret")
    parser.add_argument('--azure-storage-account-name', required=False, help="Azure Storage Account Name")
    return parser


def get_filtering_rules(args):
    """

    :param args:
    :return:
    """

    def get_filter_type_by_value(value):
        if re.search(r"^[0-9]{4}$", value) is not None:
            return "year", int(value)
        elif re.search(r"^[0-9]{4}\-[0-9]{2}$", value) is not None:
            dt = datetime.datetime.strptime(value, "%Y-%m")
            return "year-month", (dt.year, dt.month)
        elif re.search(r"^[0-9]{4}\-[0-9]{2}\-[0-9]{2}$", value) is not None:
            dt = datetime.datetime.strptime(value, "%Y-%m-%d")
            return "date", (dt.year, dt.month, dt.day)
        elif re.search(r"^[0-9]{4}\-[0-9]{2}\-[0-9]{2}\:[0-9]{4}\-[0-9]{2}\-[0-9]{2}$", value) is not None:
            dts = value.split(":")
            sdt = datetime.datetime.strptime(dts[0], "%Y-%m-%d")
            edt = datetime.datetime.strptime(dts[1], "%Y-%m-%d")
            return "range", [(sdt.year, sdt.month, sdt.day), (edt.year, edt.month, edt.day)]

    return {
        "filter_PU": get_filter_type_by_value(args.filter_pickup) if args.filter_pickup is not None else None,
        "filter_DO": get_filter_type_by_value(args.filter_dropoff) if args.filter_dropoff is not None else None,
    }

def generate_storage_options(args):
    """

    :param args:
    :return:
    """
    storage_options = {}
    if (args.azure_tenant_id is not None and
            args.azure_storage_account_name is not None and
            args.azure_client_id is not None and
            args.azure_client_secret is not None):
        storage_options = {'tenant_id': args.azure_tenant_id,
                           'client_id': args.azure_client_id,
                           'client_secret': args.azure_client_secret,
                           'account_name': args.azure_storage_account_name
                           }
    return storage_options


def generate_app_id():
    """

    :return:
    """
    return str(datetime.date.today().strftime("%Y%m%d") + "_" + secrets.token_hex(5))
