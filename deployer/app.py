import asyncio
import json
import os
import threading
from datetime import datetime, timezone

import grpc
import requests
from flask import Flask, jsonify, request
from pyzeebe import Job, ZeebeClient, ZeebeWorker

ZEEBE_ADDRESS = os.getenv("ZEEBE_ADDRESS", "zeebe:26500")
BPMN_FILE = os.getenv("BPMN_FILE", "/bpmn/wind.bpmn")
SIDDHI_CALLBACK_URL = os.getenv("SIDDHI_CALLBACK_URL", "http://siddhi:7072/middleman")


app = Flask(__name__)
client = None
worker = None
event_loop = None
process_keys = {}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def get_id(data):
    return data.get("event", {}).get("id") or data.get("id")

def get_parameterId(data):
    return data.get("event", {}).get("parameterId") or data.get("parameterId")

def get_warning_level(data):
    return data.get("event", {}).get("warning_level") or data.get("warning_level")

def callback(id, event, payload=None, error=""):
    body = {
        "id": id,
        "parameterId": get_parameterId(payload),
        "warning": get_warning_level(payload),
        "event": event,
        "timestamp": now_iso(),
        "role": "camunda",
        "payload": json.dumps(payload or {}),
        "error": error,
    }
    response = requests.post(SIDDHI_CALLBACK_URL, json=body, timeout=30)
    response.raise_for_status()




def run_async(coroutine):
    if event_loop is None:
        raise RuntimeError("Communication adapter is not ready")
    future = asyncio.run_coroutine_threadsafe(coroutine, event_loop)
    return future.result(timeout=30)


async def publish_message(name, order_id, variables):
    await client.publish_message(name, str(order_id), variables)


@app.post("/wind_data")
def receive_wind_data():
    try:
        data = request.get_json(force=True)
        id = get_id(data)
        callback(id, "WindDataReceived", payload=data)
        return jsonify({"status": "success"}), 200
    except Exception as error:
        print(f"Error processing wind data: {error}")
        return jsonify({"status": "error", "message": str(error)}), 500



async def wind_warning_task(job: Job):
    id = job.variables.get("id")
    parameterId = job.variables.get("parameterId")
    wind_speed = job.variables.get("mean_wind_speed")
    wind_gust = job.variables.get("wind_gust")
    if wind_speed > 20.8 and wind_speed <= 24.5 and wind_gust > 24.5:
        callback(id, "WindWarning", payload={"parameterId": parameterId, "warning_level": "Category 1", "mean_wind_speed": wind_speed, "wind_gust": wind_gust})
    elif wind_speed > 24.5 and wind_speed <= 32.6:
        callback(id, "WindWarning", payload={"parameterId": parameterId, "warning_level": "Category 2", "mean_wind_speed": wind_speed, "wind_gust": wind_gust})
    elif wind_speed > 32.6:
        callback(id, "WindWarning", payload={"parameterId": parameterId, "warning_level": "Category 3", "mean_wind_speed": wind_speed, "wind_gust": wind_gust})
    else:
        callback(id, "WindWarning", payload={"parameterId": parameterId, "warning_level": "No warning", "mean_wind_speed": wind_speed, "wind_gust": wind_gust})
    await job.set_success_status()


async def deploy_and_work():
    global client, worker
    channel = grpc.aio.insecure_channel(ZEEBE_ADDRESS)
    client = ZeebeClient(grpc_channel=channel)
    worker = ZeebeWorker(grpc_channel=channel)
    worker.task(task_type="wind-warning")(wind_warning_task)
    for attempt in range(1, 31):
        try:
            deployment = await client.deploy_process(BPMN_FILE)
            print(f"[Camunda] BPMN deployed: {deployment}", flush=True)
            break
        except Exception as error:
            cause = getattr(error, "__cause__", None)
            print(
                f"[Camunda] deployment attempt {attempt} failed: "
                f"{error!r}; cause={cause!r}",
                flush=True,
            )
            if attempt == 30:
                raise
            await asyncio.sleep(2)
    await worker.work()


def serve_http():
    app.run(host="0.0.0.0", port=5001, threaded=True)


async def main():
    global event_loop
    event_loop = asyncio.get_running_loop()
    threading.Thread(target=serve_http, daemon=True).start()
    await deploy_and_work()


if __name__ == "__main__":
    asyncio.run(main())
