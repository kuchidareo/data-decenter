# Data Decenter Prototype

This app is a minimal coordinator/worker example over HTTP with Docker.

- The `worker` starts, generates a UUID `worker_id`, and sends `POST /register` to the `coordinator`.
- The `coordinator` identifies that worker and stores it.
- The `coordinator` creates a task (currently sorting task)for that `worker_id` and gives that task to the specific worker.
- The `worker` computes the result and sends it back with `POST /result`.

## Required environment variables

Set these before running:

- `COORDINATOR_ID`: UUID or string for the coordinator instance
- `COORDINATOR_URL`: coordinator base URL for the worker, for example `http://coordinator:5000`

(Optional)
- `TASK_SIZE`: number of elements to sort per task (default: `10000000`)
- `TASK_INTERVAL_SECONDS`: seconds between task creation (default: `60`)
- `TASK_POLL_INTERVAL_SECONDS`: seconds between worker task polls (default: `120`)

## Run

```bash
docker compose up --build
```

