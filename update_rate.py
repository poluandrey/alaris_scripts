import requests
import logging
from urllib.parse import urljoin
import os

from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("ALARIS_USER")
password = os.getenv("PASSWORD")
alaris_url = os.getenv("ALARIS_DOMAIN")

logger = logging.getLogger("update_sms_rate")
file_handler = logging.FileHandler(filename="update_sms_rate.log")
file_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)


def authorize():
    url = urljoin(alaris_url, "auth")
    logger.debug(url)
    resp = requests.get(url, auth=HTTPBasicAuth(username=user, password=password))
    resp.raise_for_status()
    return resp.json()["token"]


def retrieve_sms_rate(token, product_id, **kwargs):
    url = urljoin(alaris_url, "sms_rate")
    logger.debug(url)
    headers = {"Authorization": f"Bearer {token}"}
    params = {"product_id": product_id}
    params.update(kwargs)
    logger.debug(params)
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


def create_rate_list_for_update(mccmnc_for_update, rate_start_date, rate_end_date):
    rates = [
        {
            "rate_start_date": rate_start_date,
            "rate_end_date": rate_end_date,
            "mccmnc": mccmnc,
            "rate": 0,
        }
        for mccmnc in mccmnc_for_update
    ]
    return rates


def update_sms_rate(token, product_id, new_rates, **kwargs):
    url = urljoin(alaris_url, "sms_rate")

    payload = {"product_id": product_id, "rows": new_rates}
    logger.debug(payload)
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    return resp.json()


def main(rate_start_date=None, rate_end_date=None):
    if not rate_start_date:
        rate_start_date = (
            (datetime.today() - timedelta(days=datetime.today().day))
            .replace(day=1)
            .strftime("%Y-%m-%d")
        )
    if not rate_end_date:
        rate_end_date = datetime.today().replace(day=1).strftime("%Y-%m-%d")
    try:
        token = authorize()
        current_rates = retrieve_sms_rate(
            token,
            product_id=14023,
            rate_start_date=rate_start_date,
            rate_end_date=rate_end_date,
            typy="between",
        )
        mccmncs = [rate["mccmnc"] for rate in current_rates]
        new_rates = create_rate_list_for_update(mccmncs, rate_start_date, rate_end_date)
        update_report = update_sms_rate(token, product_id=14023, new_rates=new_rates)
        logger.info(update_report["mini_report"])
    except requests.HTTPError as err:
        logger.exception(f"an http error\n{err}", stack_info=True)


if __name__ == "__main__":
    start_date = (
        (datetime.today() - timedelta(days=datetime.today().day))
        .replace(day=1)
        .strftime("%Y-%m-%d")
    )
    end_date = datetime.today().replace(day=1).strftime("%Y-%m-%d")
    main(rate_start_date=start_date, rate_end_date=end_date)
