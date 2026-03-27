import os
from typing import Literal, Optional
from uuid import uuid4

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

COORDINATOR_ID = os.environ["COORDINATOR_ID"]
workers: dict[str, dict[str, str]] = {}


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
    args: Optional[list[int]] = None


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


@app.post("/register", response_model=WorkerTaskResponse)
def register_worker(req: RegisterWorkerRequest):
    workers[req.worker_id] = {
        "worker_id": req.worker_id,
        "message": req.message,
    }
    task = {
        "type": "task",
        "coordinator_id": COORDINATOR_ID,
        "worker_id": req.worker_id,
        "task_id": str(uuid4()),
        "op": "add",
        "args": [2, 3],
    }
    print(f"[coordinator] registered worker={req.worker_id}, message={req.message}")
    print(f"[coordinator] dispatching task to worker={req.worker_id}: {task}")
    return WorkerTaskResponse(
        coordinator_id=COORDINATOR_ID,
        message="worker registered",
        worker_id=req.worker_id,
        registered=True,
        type=task["type"],
        task_id=task["task_id"],
        op=task["op"],
        args=task["args"],
    )


@app.post("/result", response_model=ResultResponse)
def post_result(req: ResultRequest):
    if req.worker_id not in workers:
        print(f"[coordinator] received result from unknown worker={req.worker_id}")
    result_received = req.model_dump()
    print(f"[coordinator] received result: {result_received}")
    return ResultResponse(coordinator_id=COORDINATOR_ID, status="received")
