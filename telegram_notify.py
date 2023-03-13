import os
import requests

from dotenv import load_dotenv

load_dotenv()

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")


def send_tg_message(message):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    json = {
        "chat_id": TG_CHAT_ID,
        "text": message,
        "parse_mode": "html",
    }
    tg_resp = requests.post(url, json=json)
    tg_resp.raise_for_status()


def products_formatter(products, direction):
    for _, product in enumerate(products):
        if _ == 0:
            prod_str = f"<b>{direction} products</b>: {product}\n"
        else:
            prod_str = prod_str + f"                         {product}\n"
    return prod_str


def rerating_task_formatter(task):
    if type(task["src_product_ids"]) == list:
        src_product = products_formatter(task["src_product_ids"], "src")
    else:
        src_product = f'<b>src products</b>: {task["src_product_ids"]}\n'
    if type(task["dst_product_ids"]) == list:
        dst_product = products_formatter(task["dst_product_ids"], "dst")
    else:
        dst_product = f'<b>dst products</b>: {task["dst_product_ids"]}\n'
    message = (
        f'<b>task id</b>: {task["task_id"]}\n'
        f'<b>status</b>: {task["task_status"]}\n'
        f'<b>task start time</b>: {task["task_start_time"]}\n'
        f"<b>last update time</b>: "
        f'{task["task_last_uprate_time"]}\n'
        f"{src_product}"
        f"{dst_product}"
        f'<b>rerating period</b>: from {task["rerating_start_time"]} '
        f'till {task["rerating_end_time"]}'
    )
    return message


def send_rerating_notification(tasks):
    # ToDo handle 429 Client Error: Too Many Requests
    while tasks:
        message = rerating_task_formatter(tasks.pop())
        send_tg_message(message)


if __name__ == "__main__":
    send_tg_message("Test message")
