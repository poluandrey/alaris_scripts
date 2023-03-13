import json
import logging
import os
from datetime import datetime
from typing import Any, List, Dict
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv
from requests import HTTPError
from requests.auth import HTTPBasicAuth

load_dotenv()

log_dir = os.getenv('LOG_DIR')
log_file = os.path.join(log_dir, __name__)

logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('sms_rerating_task.log')
file_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)

ALARIS_DOMAIN = os.getenv('ALARIS_DOMAIN')
ALARIS_USER = os.getenv('ALARIS_USER')
ALARIS_PASSWD = os.getenv('PASSWORD')

TASK_STATUSES = {
    1: 'new',
    0: 'ready',
    2: 'error',
    3: 'aborted',
    4: 'scheduled',
    5: 'pending',
    6: 'waiting',
    7: "in_process",
}


def get_token() -> str:
    """return auth token"""
    url = urljoin(ALARIS_DOMAIN, 'auth')
    auth_resp = requests.get(
        url, auth=HTTPBasicAuth(username=ALARIS_USER, password=ALARIS_PASSWD)
    )
    auth_resp.raise_for_status()
    return auth_resp.json()['token']


def get_tasks(session: requests.Session, task_type_id: int, **kwargs) -> Any:
    """return list of task with provided task_type_id"""
    url = urljoin(ALARIS_DOMAIN, 'task')
    payload = {'task_type_id': task_type_id}
    payload.update(kwargs)
    task_resp = session.get(url, params=payload)

    task_resp.raise_for_status()
    return task_resp.json()


def retrieve_product(session: requests.Session, product_id: str):
    url = urljoin(ALARIS_DOMAIN, 'product/')
    url = urljoin(url, product_id)
    product_resp = session.get(url)
    product_resp.raise_for_status()
    return product_resp.json()


def retrieve_carrier(session: requests.Session, car_id: str):
    url = urljoin(ALARIS_DOMAIN, 'carrier/')
    url = urljoin(url, car_id)
    car_resp = session.get(url)
    car_resp.raise_for_status()
    return car_resp.json()


def retrieve_account(session: requests.Session, acc_id: str):
    url = urljoin(ALARIS_DOMAIN, 'account/')
    url = urljoin(url, acc_id)
    acc_resp = session.get(url)
    acc_resp.raise_for_status()
    return acc_resp.json()


def get_products(session: requests.Session) -> List[Dict]:
    url = urljoin(ALARIS_DOMAIN, 'product')
    prod_resp = session.get(url)
    prod_resp.raise_for_status()
    return prod_resp.json()


def get_accounts(session: requests.Session) -> List[Dict]:
    url = urljoin(ALARIS_DOMAIN, 'account')
    acc_resp = session.get(url)
    acc_resp.raise_for_status()
    return acc_resp.json()


def get_carriers(session: requests.Session) -> List[Dict]:
    url = urljoin(ALARIS_DOMAIN, 'carrier')
    car_resp = session.get(url)
    car_resp.raise_for_status()
    return car_resp.json()


def make_session(token: str) -> requests.Session:
    logger.debug('start create new session')
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {token}'})
    return session


def check_updated_time(task, time_shift):
    logger.info('starting checking update time')
    end_time = datetime.utcnow().replace(second=0, microsecond=0)
    start_time = end_time - time_shift
    task_last_updated_time = datetime.strptime(
        task['task_last_update_time'], '%Y.%m.%d %H:%M:%S'
    )
    if start_time <= task_last_updated_time < end_time:
        logger.info('finished checking update time')
        return True
    logger.info('finished checking update time')
    return False


def get_filtered_task(tasks, time_shift):
    logger.info(time_shift)
    logger.info('starting filtering tasks')
    logger.debug(f'count of tasks for filtering {len(tasks)}')
    for task in tasks:
        logger.debug(f'task: {task}')
        if check_updated_time(task, time_shift):
            task_param_json = json.loads(task['task_param_json'])
            try:
                is_autorerating = task_param_json['autorerating']
            except KeyError:
                # ToDo undefined task. need special handler
                continue
            if is_autorerating != '1':
                task.update({'task_param_json': task_param_json})
                logger.info('finished filtering task')
                yield task


def get_products_caption(dst_product_ids, products, carriers, accounts):
    if dst_product_ids == '':
        dst_product_ids = 'All products'
    elif dst_product_ids == '0':
        dst_product_ids = 'include undefined products'
    else:
        dst_product_ids = get_product_description(
            dst_product_ids,
            products=products,
            carriers=carriers,
            accounts=accounts
        )
    return dst_product_ids


def get_product_description(product_ids, products, carriers, accounts) -> List[str]:
    logger.debug(f'product ids: {product_ids}')
    products_description = []
    for product_id in product_ids.split(','):
        logger.debug(f'collect info about product_id: {product_id}')
        if product_id == 0:
            products_description.append('include undefined product')
        else:
            carrier_name, product_descr, currency_code = retrieve_product_caption(
                int(product_id),
                products=products,
                carriers=carriers,
                accounts=accounts
            )
            products_description.append(f'{carrier_name} - {product_descr}({currency_code})')
    logger.debug(f'products description list: {products_description}')
    return products_description


def retrieve_product_caption(product_id, products, carriers, accounts):
    for sms_product in products:
        if sms_product['id'] == product_id:
            product = sms_product
            break
    product_descr = product['descr']
    car_id = product['car_id']
    acc_id = product['acc_id']
    carrier = list(filter(lambda x: x['id'] == car_id, carriers))[0]
    carrier_name = carrier['name']
    account = list(filter(lambda x: x['id'] == acc_id, accounts))[0]
    account_currency = account['currency_code']
    logger.debug(carrier_name)
    logger.debug(product_descr)
    return carrier_name, product_descr, account_currency


def extend_task_data(task, products, carriers, accounts):
    task_param_json: dict = task['task_param_json']
    try:
        task_status = TASK_STATUSES[task['task_status']]
    except KeyError:
        task_status = 'undefined'
    try:
        dst_product_ids = task_param_json['dst_product_ids']
    except KeyError:
        dst_product_ids = 'undefined'
    try:
        src_product_ids = task_param_json['src_product_ids']
    except KeyError:
        src_product_ids = 'undefined'
    try:
        rerating_start_time = task_param_json['start_date']
    except KeyError:
        rerating_start_time = 'undefined'
    try:
        rerating_end_time = task_param_json['end_date']
    except KeyError:
        rerating_end_time = 'undefined'

    dst_product_ids = get_products_caption(dst_product_ids, products=products, carriers=carriers, accounts=accounts)
    src_product_ids = get_products_caption(src_product_ids, products=products, carriers=carriers, accounts=accounts)

    task_start_time = task_param_json['task_start_time']
    if task_start_time == '':
        task_start_time = task['task_start_time']
    task_last_update_time = task['task_last_update_time']
    extended_task = {
        'task_id': task['id'],
        'task_status': task_status,
        'task_start_time': task_start_time,
        'task_last_uprate_time': task_last_update_time,
        'src_product_ids': src_product_ids,
        'dst_product_ids': dst_product_ids,
        'rerating_start_time': rerating_start_time,
        'rerating_end_time': rerating_end_time,
    }
    return extended_task


def main(time_shift):
    logger.info('start work')
    try:
        token = get_token()
    except HTTPError as err:
        logger.exception(f'an HTTP error occurred\n{err}')
        logger.warning('could not retrieve data')
        return
    http_session = make_session(token)
    try:
        with http_session as session:
            products = get_products(session)
            carriers = get_carriers(session)
            accounts = get_accounts(session)
            tasks = get_tasks(session, task_type_id=11)
        for task in get_filtered_task(tasks, time_shift):
            task = extend_task_data(task, products=products, carriers=carriers, accounts=accounts)
            yield task
    except HTTPError as err:
        if (
                err.response.status_code == 426
                and err.response.json()['error_message'] == 'Token is expired'
        ):
            logger.warning('token has expired')
            token = get_token()
            http_session = make_session(token)
            with http_session as session:
                tasks = get_tasks(session, task_type_id=11)
            for task in get_filtered_task(tasks, time_shift):
                logger.info(task)
                yield task
    logger.info('finished work')


if __name__ == '__main__':
    main(time_shift=6500)
