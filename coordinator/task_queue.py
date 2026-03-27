from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from uuid import uuid4


class TaskPriority(str, Enum):
    LATENCY_TOLERANT = "latency-tolerant"
    LATENCY_CRITICAL = "latency-critical"


@dataclass(order=True)
class Task:
    task_id: str
    op: str
    size: int
    priority: TaskPriority

    @classmethod
    def create_sort_task(
        cls, *, size: int, priority: TaskPriority, task_id: Optional[str] = None
    ) -> "Task":
        return cls(
            task_id=task_id or str(uuid4()),
            op="sort",
            size=size,
            priority=priority,
        )


class TaskQueue:
    def __init__(self, max_size: int) -> None:
        self._max_size = max_size
        self._items: list[Task] = []

    def push(self, task: Task) -> bool:
        if len(self._items) >= self._max_size:
            return False
        self._items.append(task)
        # Maintain priority ordering with stable behavior for equal priorities.
        self._items.sort()
        return True

    def pop_next(self) -> Optional[Task]:
        if not self._items:
            return None
        return self._items.pop(0)

    def __len__(self) -> int:
        return len(self._items)
