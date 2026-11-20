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
BPMN_FILE1 = os.getenv("BPMN_FILE1", "/bpmn/wind.bpmn")
BPMN_FILE2 = os.getenv("BPMN_FILE2", "/bpmn/cloudburst.bpmn")
BPMN_FILE3 = os.getenv("BPMN_FILE3", "/bpmn/snow.bpmn")
BPMN_FILE4 = os.getenv("BPMN_FILE4", "/bpmn/waterlevel.bpmn")
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
        process_variables = (
            data.get("event") if isinstance(data.get("event"), dict) else data
        )
        id = get_id(data)
        callback(id, "WindDataReceived", payload=data)
        process_instance_key = run_async(
            client.run_process("Process_wind", variables=process_variables)
        )
        return jsonify(
            {"status": "success", "process_instance_key": process_instance_key}
        ), 200
    except Exception as error:
        print(f"Error processing wind data: {error}")
        return jsonify({"status": "error", "message": str(error)}), 500


@app.post("/cloudburst_data")
def receive_cloudburst_data():
    try:
        data = request.get_json(force=True)
        process_variables = (
            data.get("event") if isinstance(data.get("event"), dict) else data
        )
        if "rain" in process_variables and "value" not in process_variables:
            process_variables["value"] = process_variables["rain"]
        id = get_id(data)
        callback(id, "CloudburstDataReceived", payload=data)
        process_instance_key = run_async(
            client.run_process("Process_cloudburst", variables=process_variables)
        )
        return jsonify(
            {"status": "success", "process_instance_key": process_instance_key}
        ), 200
    except Exception as error:
        print(f"Error processing cloudburst data: {error}")
        return jsonify({"status": "error", "message": str(error)}), 500


@app.post("/snow_data")
def receive_snow_data():
    try:
        data = request.get_json(force=True)
        process_variables = (
            data.get("event") if isinstance(data.get("event"), dict) else data
        )
        if "snow" in process_variables and "value" not in process_variables:
            process_variables["value"] = process_variables["snow"]
        id = get_id(data)
        callback(id, "SnowDataReceived", payload=data)
        process_instance_key = run_async(
            client.run_process("Process_Snow", variables=process_variables)
        )
        return jsonify(
            {"status": "success", "process_instance_key": process_instance_key}
        ), 200
    except Exception as error:
        print(f"Error processing snow data: {error}")
        return jsonify({"status": "error", "message": str(error)}), 500

@app.post("/waterlevel_data")
def receive_waterlevel_data():
    try:
        data = request.get_json(force=True)
        process_variables = (
            data.get("event") if isinstance(data.get("event"), dict) else data
        )
        if "waterlevel" in process_variables and "water_level" not in process_variables:
            process_variables["water_level"] = process_variables["waterlevel"]
        id = get_id(data)
        callback(id, "WaterLevelDataReceived", payload=data)
        process_instance_key = run_async(
            client.run_process("Process_waterlevel", variables=process_variables)
        )
        return jsonify(
            {"status": "success", "process_instance_key": process_instance_key}
        ), 200
    except Exception as error:
        print(f"Error processing water level data: {error}")
        return jsonify({"status": "error", "message": str(error)}), 500



async def warning_complete(job: Job, id, parameterId, warning_level="Unknown"):
    
    if parameterId == "wind_data":
        warning = "WindWarning"
    elif parameterId == "cloudburst_data":
        warning = "CloudburstWarning"
    elif parameterId == "snow_data":
        warning = "SnowWarning"
    elif parameterId == "waterlevel_data":
        warning = "WaterLevelWarning"

    else: 
        warning = f"UnknownWarning -- parameterId: {parameterId}"
    callback(id, warning, payload={"parameterId": parameterId, "warning_level": warning_level})
    await job.set_success_status()


async def deploy_and_work():
    global client, worker
    channel = grpc.aio.insecure_channel(ZEEBE_ADDRESS)
    client = ZeebeClient(grpc_channel=channel)
    worker = ZeebeWorker(grpc_channel=channel)
    worker.task(
        task_type="send-warning-internal",
        variables_to_fetch=["id", "parameterId", "warning_level"],
    )(warning_complete)
    for attempt in range(1, 31):
        try:
            deployment = await client.deploy_process(BPMN_FILE1, BPMN_FILE2, BPMN_FILE3, BPMN_FILE4)
            print(f"[Camunda] BPMNs deployed: {deployment}", flush=True)
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
