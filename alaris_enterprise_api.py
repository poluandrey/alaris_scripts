import os
from typing import List

import requests

from dotenv import load_dotenv

from logger import create_logger

load_dotenv()
logger = create_logger(__name__, "alaris_enterprise_api.log")

EAPI_URL = os.getenv("ALARIS_EAPI_DOMAIN")
EAPI_USER = os.getenv("ALARIS_EAPI_USER")


class EAPIError(Exception):
    """
    EAPI main exception
    """

    def __init__(self, message):
        super().__init__(message)


def get_raw_sms_rates(product: str, start_date: str, end_date: str, **kwargs) -> List:
    """
    Getting the list of rates for products, MCCMNCs and dates.

    :param product: comma separated string of product id
    :param start_date: rate start_date in YYYY-MM-DD format
    :param end_date:  rate end_date in YYYY-MM-DD format
    :param kwargs: mccmnc_list comma separated string of mccmnc

    :return:
    """
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "Enterprise.Auto",
        "params": {
            "name": "get_raw_sms_rate_list",
            "args": {
                "product_list": product,
                "start_date": start_date,
                "end_date": end_date,
                "mccmnc_list": kwargs.get("mccmnc_list", ""),
            },
            "auth": "YS5wb2x1bWVzdG55aTpWcXAqR3cycw==",
        },
    }
    rate_resp = requests.post(EAPI_URL, json=payload)
    logger.info(payload)
    rate_resp.raise_for_status()
    check_eapi_answer(rate_resp.json())
    return rate_resp.json()["result"]["data"]


def check_eapi_answer(answer: dict):
    """
    Check EAPI answer and raise EAPIError if it contains 'error' key
    :param answer: EAPI answer
    :return:
    """
    error = answer.get("error")
    if error:
        raise EAPIError(error["message"])
