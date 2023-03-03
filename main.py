import argparse

from update_rate import main as sms_rate_update


def sms_rate_update_callback(arguments):
    sms_rate_update(
        rate_start_date=arguments.rate_start_date, rate_end_date=arguments.rate_end_date
    )


def argument_parser():
    parser = argparse.ArgumentParser(
        description="collection of commands for working with sms"
    )
    sub_parser = parser.add_subparsers(
        help="list of allowed commands",
    )
    rate_cmd = sub_parser.add_parser(
        "rate",
        help="Set to zero rate for previous month for product "
        '"Retail Demo Client Premium". All open rate start '
        "date will be set to the start of the month",
    )
    rate_cmd.add_argument("--rate-start-date", dest="rate_start_date", required=False)
    rate_cmd.add_argument("--rate-end-date", dest="rate_end_date", required=False)
    rate_cmd.set_defaults(callback=sms_rate_update_callback)
    return parser.parse_args()


if __name__ == "__main__":
    args = argument_parser()
    print(args)
    args.callback(args)
