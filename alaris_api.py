import os
from typing import Any, List, Dict
from urllib.parse import urljoin
from dotenv import load_dotenv

import requests
from requests.auth import HTTPBasicAuth


load_dotenv()

ALARIS_DOMAIN = os.getenv("ALARIS_DOMAIN")
ALARIS_USER = os.getenv("ALARIS_USER")
ALARIS_PASSWD = os.getenv("PASSWORD")


def get_token() -> str:
    """return auth token"""
    url = urljoin(ALARIS_DOMAIN, "auth")
    auth_resp = requests.get(
        url, auth=HTTPBasicAuth(username=ALARIS_USER, password=ALARIS_PASSWD)
    )
    auth_resp.raise_for_status()
    return auth_resp.json()["token"]


def get_tasks(session: requests.Session, task_type_id: int, **kwargs) -> Any:
    """return list of task with provided task_type_id"""
    url = urljoin(ALARIS_DOMAIN, "task")
    payload = {"task_type_id": task_type_id}
    payload.update(kwargs)
    task_resp = session.get(url, params=payload)

    task_resp.raise_for_status()
    return task_resp.json()


def retrieve_product(session: requests.Session, product_id: str):
    url = urljoin(ALARIS_DOMAIN, "product/")
    url = urljoin(url, product_id)
    product_resp = session.get(url)
    product_resp.raise_for_status()
    return product_resp.json()


def retrieve_carrier(session: requests.Session, car_id: str):
    url = urljoin(ALARIS_DOMAIN, "carrier/")
    url = urljoin(url, car_id)
    car_resp = session.get(url)
    car_resp.raise_for_status()
    return car_resp.json()


def retrieve_account(session: requests.Session, acc_id: str):
    url = urljoin(ALARIS_DOMAIN, "account/")
    url = urljoin(url, acc_id)
    acc_resp = session.get(url)
    acc_resp.raise_for_status()
    return acc_resp.json()


def get_products(session: requests.Session) -> List[Dict]:
    url = urljoin(ALARIS_DOMAIN, "product")
    prod_resp = session.get(url)
    prod_resp.raise_for_status()
    return prod_resp.json()


def get_accounts(session: requests.Session) -> List[Dict]:
    url = urljoin(ALARIS_DOMAIN, "account")
    acc_resp = session.get(url)
    acc_resp.raise_for_status()
    return acc_resp.json()


def get_carriers(session: requests.Session) -> List[Dict]:
    url = urljoin(ALARIS_DOMAIN, "carrier")
    car_resp = session.get(url)
    car_resp.raise_for_status()
    return car_resp.json()


def make_session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session


def retrieve_sms_rate(session, product_id, **kwargs):
    url = urljoin(ALARIS_DOMAIN, "sms_rate")
    params = {"product_id": product_id}
    params.update(kwargs)
    resp = session.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


def update_sms_rate(session, product_id, new_rates):
    url = urljoin(ALARIS_DOMAIN, "sms_rate")

    payload = {"product_id": product_id, "rows": new_rates}
    resp = session.post(url, json=payload)
    resp.raise_for_status()
    return resp.json()
