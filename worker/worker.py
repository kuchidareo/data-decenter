import os
import time
from uuid import uuid4

import requests

COORDINATOR = os.environ["COORDINATOR_URL"]
WORKER_ID = str(uuid4())


def wait_for_coordinator():
    while True:
        try:
            response = requests.get(f"{COORDINATOR}/", timeout=2)
            response.raise_for_status()
            print("[worker] coordinator is ready")
            return
        except requests.RequestException:
            print("[worker] waiting for coordinator...")
            time.sleep(1)


def register_worker_and_get_task():
    response = requests.post(
        f"{COORDINATOR}/register",
        json={
            "worker_id": WORKER_ID,
            "message": "hello",
        },
        timeout=5,
    )
    response.raise_for_status()
    assigned_task = response.json()
    print(f"[worker] assigned task response: {assigned_task}")
    return assigned_task


def compute(task: dict):
    if task["type"] != "task":
        return None

    if task["op"] == "add":
        a, b = task["args"]
        return {
            "worker_id": WORKER_ID,
            "task_id": task["task_id"],
            "result": a + b,
        }

    raise ValueError(f"unsupported op: {task['op']}")


def send_result(result: dict):
    response = requests.post(
        f"{COORDINATOR}/result",
        json=result,
        timeout=5,
    )
    response.raise_for_status()
    print(f"[worker] result response: {response.json()}")


if __name__ == "__main__":
    wait_for_coordinator()

    assigned_task = register_worker_and_get_task()
    print(f"[worker] worker_id={WORKER_ID}, coordinator_id={assigned_task['coordinator_id']}")
    print(f"[worker] task_id={assigned_task['task_id']}, op={assigned_task['op']}, args={assigned_task['args']}")

    result = compute(assigned_task)

    if result is not None:
        send_result(result)
