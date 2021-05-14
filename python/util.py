import datetime
import re
import secrets
import sys


def info(msg):
    """
    Logs info message.
    :param msg: message
    """
    print("{} - {} ".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))


def error(msg):
    """
    Logs error message.
    :param msg:
    """
    print(msg, file=sys.stderr)


def get_filtering_rules(args):
    """
    Return filtering rules from arguments.
    :param args: arguments
    :return:filtering rules from arguments.
    """

    def get_filter_type_by_value(value):
        if re.search(r"^[0-9]{4}$", value) is not None:
            return {"type": "year", "value": int(value)}
        elif re.search(r"^[0-9]{4}\-[0-9]{2}$", value) is not None:
            dt = datetime.datetime.strptime(value, "%Y-%m")
            return {"type": "year-month", "value": (dt.year, dt.month)}
        elif re.search(r"^[0-9]{4}\-[0-9]{2}\-[0-9]{2}$", value) is not None:
            dt = datetime.datetime.strptime(value, "%Y-%m-%d")
            return {"type": "date", "value": (dt.year, dt.month, dt.day)}
        elif re.search(r"^[0-9]{4}\-[0-9]{2}\-[0-9]{2}\:[0-9]{4}\-[0-9]{2}\-[0-9]{2}$", value) is not None:
            dts = value.split(":")
            sdt = datetime.datetime.strptime(dts[0], "%Y-%m-%d")
            edt = datetime.datetime.strptime(dts[1], "%Y-%m-%d")
            return {"type": "range", "value": [(sdt.year, sdt.month, sdt.day), (edt.year, edt.month, edt.day)]}
        else:
            raise NotImplementedError("Unsupported format : {}".format(value))

    rules = []
    if args.filter_pickup is not None:
        rule = get_filter_type_by_value(args.filter_pickup)
        rule["column"] = "datetime_PU"
        rules.append(rule)
    if args.filter_dropoff is not None:
        rule = get_filter_type_by_value(args.filter_dropoff)
        rule["column"] = "datetime_DO"
        rules.append(rule)
    return rules


def filtering_rules_to_expression(filtering_rules, rule_to_expression_fun):
    """
    Returns single SQL where like expression expression capturing all filtering rules (Pandas,Dask,PySpark)
    :param filtering_rules: filtering function
    :param rule_to_expression_fun:  rule to expression function
    :return: SQL where like expression
    """
    total_rules = len(filtering_rules)

    if total_rules == 0:
        return None
    elif total_rules == 1:
        return rule_to_expression_fun(filtering_rules[0])
    else:
        expression = rule_to_expression_fun(filtering_rules.pop(0))
        for rule in filtering_rules:
            expression = expression + " and " + rule_to_expression_fun(rule)
        return expression


def generate_storage_options(args):
    """
    Generate stoage options from args
    :param args:
    :return:
    """

    if (not args.azure_tenant_id or
            not args.azure_storage_account_name or
            not args.azure_client_id or
            not args.azure_client_secret):
        return {}
    else:
        return {'tenant_id': args.azure_tenant_id,
                'client_id': args.azure_client_id,
                'client_secret': args.azure_client_secret,
                'account_name': args.azure_storage_account_name}


def init_parser(parser):
    """
    Generate stoage options from args
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
    parser.add_argument('--azure-tenant-id', required=False, help="Azure Tenant id", default="")
    parser.add_argument('--azure-client-id', required=False, help="Azure Client id", default="")
    parser.add_argument('--azure-client-secret', required=False, help="Azure Client Secret", default="")
    parser.add_argument('--azure-storage-account-name', required=False, help="Azure Storage Account Name", default="")
    return parser


def generate_app_id():
    """

    :return:
    """
    return str(datetime.date.today().strftime("%Y%m%d") + "_" + secrets.token_hex(5))
