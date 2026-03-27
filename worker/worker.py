import os
import random
import time
from uuid import uuid4

import requests

from sorting import quicksort

COORDINATOR = os.environ["COORDINATOR_URL"]
WORKER_ID = str(uuid4())
TASK_POLL_INTERVAL_SECONDS = int(os.getenv("TASK_POLL_INTERVAL_SECONDS", "120"))

print(f"[worker] worker_id={WORKER_ID}")

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
    return assigned_task


def compute(task: dict):
    if task["type"] != "task":
        return None

    if task["op"] == "sort":
        size = int(task["size"])
        data = [random.randint(0, size) for _ in range(size)]
        quicksort(data)
        result_value = data[:20] if data else 0
        return {
            "worker_id": WORKER_ID,
            "task_id": task["task_id"],
            "result": result_value,
        }

    raise ValueError(f"unsupported op: {task['op']}")


def send_result(result: dict):
    response = requests.post(
        f"{COORDINATOR}/result",
        json=result,
        timeout=5,
    )
    response.raise_for_status()


if __name__ == "__main__":
    wait_for_coordinator()

    while True:
        assigned_task = register_worker_and_get_task()
        print(f"[worker] assigned task response: {assigned_task}")
        if assigned_task["type"] == "task":
            print(
                f"[worker] task_id={assigned_task['task_id']}, op={assigned_task['op']}, "
                f"size={assigned_task['size']}, priority={assigned_task['priority']}"
            )
        else:
            print("[worker] no task assigned")
        
        print(f"[worker] starting task {assigned_task['task_id']}")
        result = compute(assigned_task)
        print(f"[worker] finished task {assigned_task['task_id']} result: {result}")

        if result is not None:
            send_result(result)
            print(f"[worker] sent result for task {assigned_task['task_id']} to coordinator")

        time.sleep(TASK_POLL_INTERVAL_SECONDS)
