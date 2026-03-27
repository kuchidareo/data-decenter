import asyncio
import os
import random
from typing import Literal, Optional

from fastapi import FastAPI
from pydantic import BaseModel

from task_queue import Task, TaskPriority, TaskQueue

app = FastAPI()

COORDINATOR_ID = os.environ["COORDINATOR_ID"]
workers: dict[str, dict[str, str]] = {}
task_queue = TaskQueue(max_size=10)

TASK_SIZE = int(os.getenv("TASK_SIZE", "10000000"))
TASK_INTERVAL_SECONDS = int(os.getenv("TASK_INTERVAL_SECONDS", "60"))


async def task_producer() -> None:
    while True:
        await asyncio.sleep(TASK_INTERVAL_SECONDS)
        priority = random.choice(
            [TaskPriority.LATENCY_TOLERANT, TaskPriority.LATENCY_CRITICAL]
        )
        created = task_queue.push(
            Task.create_sort_task(size=TASK_SIZE, priority=priority)
        )
        if created:
            print(
                f"[coordinator] queued task priority={priority}, size={TASK_SIZE}"
            )
        else:
            print("[coordinator] queue full; skipping task creation")


class RegisterWorkerRequest(BaseModel):
    worker_id: str
    message: str


class WorkerTaskResponse(BaseModel):
    coordinator_id: str
    message: str
    worker_id: str
    registered: bool
    type: Literal["task", "no_task"]
    task_id: Optional[str] = None
    op: Optional[str] = None
    size: Optional[int] = None
    priority: Optional[TaskPriority] = None


class ResultRequest(BaseModel):
    worker_id: str
    task_id: str
    result: int


class ResultResponse(BaseModel):
    coordinator_id: str
    status: str


@app.get("/")
def index():
    return {
        "service": "coordinator",
        "status": "ok",
        "coordinator_id": COORDINATOR_ID,
    }


@app.on_event("startup")
async def startup() -> None:
    asyncio.create_task(task_producer())


@app.post("/register", response_model=WorkerTaskResponse)
def register_worker(req: RegisterWorkerRequest):
    workers[req.worker_id] = {
        "worker_id": req.worker_id,
        "message": req.message,
    }
    task = task_queue.pop_next()
    print(f"[coordinator] registered worker={req.worker_id}, message={req.message}")
    if task is None:
        print(f"[coordinator] no task available for worker={req.worker_id}")
        return WorkerTaskResponse(
            coordinator_id=COORDINATOR_ID,
            message="worker registered",
            worker_id=req.worker_id,
            registered=True,
            type="no_task",
        )

    print(f"[coordinator] dispatching task to worker={req.worker_id}: {task}")
    return WorkerTaskResponse(
        coordinator_id=COORDINATOR_ID,
        message="worker registered",
        worker_id=req.worker_id,
        registered=True,
        type="task",
        task_id=task.task_id,
        op=task.op,
        size=task.size,
        priority=task.priority,
    )


@app.post("/result", response_model=ResultResponse)
def post_result(req: ResultRequest):
    if req.worker_id not in workers:
        print(f"[coordinator] received result from unknown worker={req.worker_id}")
    result_received = req.model_dump()
    print(f"[coordinator] received result: {result_received}")
    return ResultResponse(coordinator_id=COORDINATOR_ID, status="received")
