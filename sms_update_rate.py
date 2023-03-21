import requests

from requests import HTTPError

import alaris_api
import alaris_enterprise_api as eapi
from logger import create_logger


logger = create_logger(__name__, "sms_update_rate.log")


def collect_rate_list_for_update(mccmnc_for_update, rate_start_date, rate_end_date):
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


def update_sms_rate(rate_start_date, rate_end_date, **kwargs):
    logger.info(f"rate_start_date: {rate_start_date}")
    logger.info(f"rate_end_date: {rate_end_date}")
    logger.info(f"additional args {kwargs}")
    codes = kwargs.get("codes", [])
    mccmnc = '' if not codes else ','.join(codes)

    try:
        token = alaris_api.get_token()
    except HTTPError as err:
        logger.exception(f"an HTTP error occurred\n{err}")
        return
    try:
        session = alaris_api.make_session(token=token)
        current_rates = eapi.get_raw_sms_rates(
            product="14023",
            start_date=rate_start_date,
            end_date=rate_end_date,
            mccmnc_list=mccmnc,
        )
        logger.debug(f"rate count for update {len(current_rates)}")
        logger.debug(f"raw rates: {current_rates}")
        mccmncs = set(rate["mccmnc"] for rate in current_rates)
        new_rates = collect_rate_list_for_update(
            mccmncs, rate_start_date, rate_end_date
        )
        logger.debug(new_rates)
        update_report = alaris_api.update_sms_rate(
            session, product_id=14023, new_rates=new_rates
        )
        logger.info(update_report["mini_report"])
        return update_report
    except requests.HTTPError as err:
        logger.exception(f"an http error\n{err}", stack_info=True)


if __name__ == "__main__":
    update_sms_rate(
        rate_start_date="2023-01-01", rate_end_date="2023-03-23", codes=[289088]
    )
