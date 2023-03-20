import argparse
import sys

from datetime import timedelta, datetime

from logger import create_logger
from sms_update_rate import update_sms_rate
from sms_rerating_task import main as get_rerating_task
from telegram_notify import send_rerating_notification, send_tg_message

logger = create_logger(__name__, "main.log")


def sms_rate_update_callback(arguments):
    logger.info("start update rate command")
    update_report = update_sms_rate(
        rate_start_date=arguments.rate_start_date,
        rate_end_date=arguments.rate_end_date,
        codes=arguments.codes,
    )
    if arguments.notify:
        message = (
            "Completed updating rate for product Retail Demo Client Premium\n"
            f"{update_report}"
        )
        send_tg_message(message)
    else:
        print(update_report, file=sys.stderr)
    logger.info("finished update rate command")


def rerating_task_callback(arguments):
    logger.info("start rerating command")
    rerating_tasks = list(get_rerating_task(arguments.time_shift))
    logger.debug(rerating_tasks)
    if not arguments.notify:
        for task in rerating_tasks:
            print(task, file=sys.stderr)
    else:
        send_rerating_notification(rerating_tasks)
    logger.info("finished rerating command")


def argument_parser():
    parser = argparse.ArgumentParser(
        description="collection of commands for working with sms"
    )
    sub_parser = parser.add_subparsers(
        help="list of allowed commands",
    )
    parser.add_argument("--notify", required=False, type=bool, default=False)
    rate_cmd = sub_parser.add_parser(
        "rate",
        help="Set to zero rate for previous month for product "
        '"Retail Demo Client Premium". All open rate start '
        "date will be set to the start of the month",
    )
    rate_cmd.add_argument(
        "--rate-start-date",
        dest="rate_start_date",
        required=False,
        default=datetime.now().replace(day=1).strftime("%Y-%m-%d"),
        help="rate start date in format YYYY-MM-DD. Default value first date of current month.",
    )
    rate_cmd.add_argument(
        "--rate-end-date",
        dest="rate_end_date",
        required=False,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="rate end date in format YYYY-MM-DD. Default value current day",
    )
    rate_cmd.add_argument(
        "--mccmnc-list",
        dest="codes",
        required=False,
        nargs='+',
        help="comma separated mccmnc list for filtering rate"
    )
    rate_cmd.set_defaults(callback=sms_rate_update_callback)

    rerating_task_cmd = sub_parser.add_parser(
        "rerating-task",
        help="return manual created rerating tasks that were updated. "
        "By default check tasks that were updated 1 minute ago."
        "You could specify --time-shift option to change default behavior",
    )
    rerating_task_cmd.add_argument(
        "--time-shift",
        help="time in minutes before the present time truncated to minutes",
        dest="time_shift",
        required=False,
        type=lambda d: timedelta(minutes=int(d)),
        default=timedelta(minutes=1),
    )
    rerating_task_cmd.set_defaults(callback=rerating_task_callback)
    return parser.parse_args()


if __name__ == "__main__":
    args = argument_parser()
    args.callback(args)
