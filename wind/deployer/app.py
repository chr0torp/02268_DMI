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
BPMN_FILE5 = os.getenv("BPMN_FILE5", "/bpmn/authority_warning.bpmn")
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

def get_station_id(data):
    if not isinstance(data, dict):
        return None
    return data.get("event", {}).get("stationId") or data.get("stationId")

def callback(id, event, payload=None, error=""):
    body = {
        "id": id,
        "parameterId": get_parameterId(payload),
        "stationId": get_station_id(payload),
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


################# AUTHORITY WARNINGS #########################

def format_warning_category(severity: int) -> str:
    return f"Category {severity}" if severity > 0 else "None"


@app.post("/authority_warning")
def receive_authority_warning():
    try:
        data = request.get_json(force=True)
        payload = data.get("event") if isinstance(data.get("event"), dict) else data
        event_id = get_id(data)
        station_id = str(payload["stationId"])
        severity = int(payload["severity"])
        variables = {
            **payload,
            "stationId": station_id,
            "severity": severity,
            "prev_severity": severity,
            "warning_level": format_warning_category(severity),
        }
        callback(event_id, "AuthorityWeatherEventReceived", payload=variables)
        run_async(publish_message("msg_weather_event", station_id, variables))
        return jsonify({"status": "success", "stationId": station_id, "severity": severity}), 200
    except Exception as error:
        print(f"Error starting authority warning: {error}", flush=True)
        return jsonify({"status": "error", "message": str(error)}), 500


@app.post("/authority_warning_update")
def receive_authority_warning_update():
    try:
        data = request.get_json(force=True)
        payload = data.get("event") if isinstance(data.get("event"), dict) else data
        event_id = get_id(data)
        station_id = str(payload["stationId"])
        severity = int(payload["severity"])
        variables = {
            **payload,
            "stationId": station_id,
            "severity": severity,
            "warning_level": format_warning_category(severity),
        }
        callback(event_id, "AuthorityWeatherUpdateReceived", payload=variables)
        run_async(publish_message("msg_weather_update", station_id, variables))
        return jsonify({"status": "success", "stationId": station_id, "severity": severity}), 200
    except Exception as error:
        print(f"Error updating authority warning: {error}", flush=True)
        return jsonify({"status": "error", "message": str(error)}), 500

async def notify_authority_warning(job: Job, id, parameterId, stationId, severity):
    category = format_warning_category(int(severity))
    await asyncio.to_thread(
        callback,
        id,
        "AuthorityWarningNotified",
        payload={
            "parameterId": parameterId,
            "stationId": stationId,
            "severity": severity,
            "warning_level": category,
        },
    )
    return {"prev_severity": int(severity), "warning_level": category}


async def notify_authority_dewarning(job: Job, id, parameterId, stationId, severity):
    await asyncio.to_thread(
        callback,
        id,
        "AuthorityDewarningNotified",
        payload={
            "parameterId": parameterId,
            "stationId": stationId,
            "severity": int(severity),
            "warning_level": "None",
        },
    )
    return {"warning_level": "None"}

async def warning_complete(job: Job, id, parameterId, warning_level="Unknown", stationId=None):
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

    await asyncio.to_thread(
        callback,
        id,
        warning,
        payload={
            "parameterId": parameterId, 
            "warning_level": warning_level,
            "stationId": stationId
        },
    )


async def deploy_and_work():
    global client, worker
    for attempt in range(1, 46):
        try:
            channel = grpc.aio.insecure_channel(ZEEBE_ADDRESS)
            client = ZeebeClient(grpc_channel=channel)
            worker = ZeebeWorker(grpc_channel=channel)
            worker.task(
                task_type="send-warning-internal",
                variables_to_fetch=["id", "parameterId", "warning_level", "stationId"],
            )(warning_complete)
            worker.task(
                task_type="notify-authority-warning",
                variables_to_fetch=["id", "parameterId", "stationId", "severity"],
            )(notify_authority_warning)

            worker.task(
                task_type="notify-authority-dewarning",
                variables_to_fetch=["id", "parameterId", "stationId", "severity"],
            )(notify_authority_dewarning)

            deployment = await client.deploy_process(
                BPMN_FILE1, BPMN_FILE2, BPMN_FILE3, BPMN_FILE4, BPMN_FILE5
            )
            print(f"[Camunda] BPMNs deployed: {deployment}", flush=True)
            break
        except Exception as error:
            cause = getattr(error, "__cause__", None)
            print(
                f"[Camunda] deployment attempt {attempt} failed: "
                f"{error!r}; cause={cause!r}",
                flush=True,
            )
            if attempt == 45:
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
