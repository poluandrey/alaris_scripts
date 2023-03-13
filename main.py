import argparse
import logging
import sys
import os

from datetime import timedelta
from dotenv import load_dotenv

from update_rate import main as sms_rate_update
from sms_rerating_task import main as get_rerating_task
from telegram_notify import send_rerating_notification


load_dotenv()
log_dir = os.getenv('LOG_DIR')
log_file = os.path.join(log_dir, 'main.log')

logger = logging.getLogger(log_file)
file_handler = logging.FileHandler(filename="main.log")
file_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)


def sms_rate_update_callback(arguments):
    sms_rate_update(
        rate_start_date=arguments.rate_start_date, rate_end_date=arguments.rate_end_date
    )


def rerating_task_callback(arguments):
    logger.info("start rerating command")
    rerating_tasks = list(get_rerating_task(arguments.time_shift))
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
    rate_cmd.add_argument("--rate-start-date", dest="rate_start_date", required=False)
    rate_cmd.add_argument("--rate-end-date", dest="rate_end_date", required=False)
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
