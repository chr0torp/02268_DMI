from flask import Flask, request, jsonify
from datetime import datetime, timezone
from pathlib import Path
import json
import xml.etree.ElementTree as ET

app = Flask(__name__)

LOG_DIR = Path("/logs")
XES_DIR = Path("/XES")
LOG_DIR.mkdir(parents=True, exist_ok=True)
XES_DIR.mkdir(parents=True, exist_ok=True)

RUN_ID = datetime.now(timezone.utc).strftime("%y%m%d_%H%M%S")
EVENT_STORE = LOG_DIR / f"{RUN_ID}_events.json"
XES_FILE = XES_DIR / f"{RUN_ID}_process_log.xes"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_event_payload(data):
    event_data = data.get("event") if isinstance(data.get("event"), dict) else data
    raw_payload = event_data.get("payload")
    if isinstance(raw_payload, str):
        try:
            payload = json.loads(raw_payload)
        except (TypeError, ValueError):
            payload = {"raw": raw_payload}
    elif isinstance(raw_payload, dict):
        payload = raw_payload.copy()
    else:
        payload = {}

    if not isinstance(payload, dict):
        payload = {"raw": payload}
    if isinstance(payload.get("event"), dict) and len(payload) == 1:
        payload = payload["event"]

    is_weather_observation = (
        isinstance(event_data.get("parameterId"), str)
        and event_data.get("event") is None
    )
    if not payload:
        payload = {
            key: event_data[key]
            for key in (
                "id",
                "parameterId",
                "stationId",
                "observed",
                "mean_wind_speed",
                "wind_gust",
            )
            if key in event_data and event_data[key] is not None
        }
    for key in ("parameterId", "warning"):
        if event_data.get(key) is not None:
            payload.setdefault(key, event_data[key])

    return {
        "case_id": str(
            event_data.get("case_id")
            or event_data.get("stationId")
            or event_data.get("id")
            or "UNKNOWN_CASE"
        ),
        "event": (
            event_data.get("event")
            if isinstance(event_data.get("event"), str)
            else "WeatherObserved" if is_weather_observation else "unknown"
        ),
        "timestamp": str(
            event_data.get("timestamp")
            or event_data.get("observed")
            or utc_now_iso()
        ),
        "role": str(event_data.get("role") or ("DMI" if is_weather_observation else "system")),
        "error": str(event_data.get("error") or ""),
        "payload": payload,
    }

def write_json_store(events):
    with open(EVENT_STORE, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)


def build_xes(events):
    root = ET.Element(
        "log",
        {
            "xes.version": "2.0",
            "xes.features": "nested-attributes",
            "openxes.version": "1.0RC7",
        },
    )

    ET.SubElement(
        root,
        "extension",
        {
            "name": "concept",
            "prefix": "concept",
            "uri": "http://www.xes-standard.org/concept.xesext",
        },
    )
    ET.SubElement(
        root,
        "extension",
        {
            "name": "time",
            "prefix": "time",
            "uri": "http://www.xes-standard.org/time.xesext",
        },
    )
    ET.SubElement(
        root,
        "extension",
        {
            "name": "org",
            "prefix": "org",
            "uri": "http://www.xes-standard.org/org.xesext",
        },
    )

    global_trace = ET.SubElement(root, "global", {"scope": "trace"})
    ET.SubElement(global_trace, "string", {"key": "concept:name", "value": "name"})

    global_event = ET.SubElement(root, "global", {"scope": "event"})
    ET.SubElement(global_event, "string", {"key": "concept:name", "value": "name"})
    ET.SubElement(global_event, "date", {"key": "time:timestamp", "value": "2024-01-01T00:00:00.000+00:00"})
    ET.SubElement(global_event, "string", {"key": "org:resource", "value": "resource"})

    ET.SubElement(root, "classifier", {"name": "Event Name", "keys": "concept:name"})

    grouped = {}
    for event in events:
        case_id = event.get("case_id", "UNKNOWN_CASE")
        grouped.setdefault(case_id, []).append(event)

    for case_id, case_events in grouped.items():
        trace = ET.SubElement(root, "trace")
        ET.SubElement(trace, "string", {"key": "concept:name", "value": str(case_id)})

        for event in case_events:
            event_el = ET.SubElement(trace, "event")
            ET.SubElement(event_el, "string", {"key": "concept:name", "value": str(event.get("event", "unknown"))})
            ET.SubElement(event_el, "date", {"key": "time:timestamp", "value": str(event.get("timestamp", utc_now_iso()))})
            ET.SubElement(event_el, "string", {"key": "org:resource", "value": str(event.get("role", "system"))})

            if event.get("order_id"):
                ET.SubElement(event_el, "string", {"key": "order_id", "value": str(event["order_id"])})
            if event.get("case_id"):
                ET.SubElement(event_el, "string", {"key": "case_id", "value": str(event["case_id"])})
            if event.get("error"):
                ET.SubElement(event_el, "string", {"key": "error", "value": str(event["error"])})

            payload = event.get("payload") or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (TypeError, ValueError):
                    payload = {"raw": payload}

            if isinstance(payload, dict):
                for key, value in payload.items():
                    if value is None:
                        continue
                    ET.SubElement(event_el, "string", {"key": f"payload:{key}", "value": str(value)})
            elif payload is not None:
                ET.SubElement(event_el, "string", {"key": "payload:raw", "value": str(payload)})

    ET.indent(root)
    tree = ET.ElementTree(root)
    tree.write(XES_FILE, encoding="utf-8", xml_declaration=True)


@app.route("/log", methods=["POST"])
def log_event():
    data = request.get_json(silent=True) or {}
    if not data:
        return jsonify({"status": "invalid", "message": "No JSON payload received"}), 400

    event = normalize_event_payload(data)

    try:
        if EVENT_STORE.exists():
            with open(EVENT_STORE, "r", encoding="utf-8") as f:
                events = json.load(f)
        else:
            events = []
    except (json.JSONDecodeError, OSError):
        events = []

    events.append(event)
    write_json_store(events)
    build_xes(events)

    print(f"[XES] Logged event={event['event']} case_id={event['case_id']} ts={event['timestamp']}", flush=True)
    return jsonify({"status": "logged", "xes_file": str(XES_FILE)}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5010)
