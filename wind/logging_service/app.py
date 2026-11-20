from flask import Flask, request, jsonify
from datetime import datetime, timezone
from pathlib import Path
import json
import xml.etree.ElementTree as ET

app = Flask(__name__)

LOG_DIR = Path("/logs")
LOG_DIR_WARN = Path("/logs/warnings")
XES_DIR = Path("/XES")
XES_DIR_WARN = Path("/XES/warnings")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR_WARN.mkdir(parents=True, exist_ok=True)
XES_DIR.mkdir(parents=True, exist_ok=True)
XES_DIR_WARN.mkdir(parents=True, exist_ok=True)

RUN_ID = datetime.now(timezone.utc).strftime("%y%m%d_%H%M%S")
EVENT_STORE = LOG_DIR / f"{RUN_ID}_events.json"
EVENT_STORE_WARN = LOG_DIR_WARN / f"{RUN_ID}_warnings.json"
XES_FILE = XES_DIR / f"{RUN_ID}_process_log.xes"
XES_FILE_WARN = XES_DIR_WARN / f"{RUN_ID}_warnings_log.xes"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_event_payload(data):
    print(f"[XES] Normalizing event payload:")
    event_data = data.get("event") if isinstance(data.get("event"), dict) else data
    print(f"[XES] Normalizing event payload: {event_data}", flush=True)
    
    payload = {}
    raw_payload = event_data.get("payload")
    
    if isinstance(raw_payload, str):
        try:
            payload = json.loads(raw_payload)
        except (TypeError, ValueError):
            pass
    elif isinstance(raw_payload, dict):
        payload = raw_payload.copy()

    for key in ("id", "parameterId", "warning", "mean_wind_speed", "wind_gust", "rain", "snow", "waterlevel", "stationId", "area"):
        value = event_data.get(key)
        if value is not None and value != "undefined":
            payload.setdefault(key, event_data[key])


    return {
        "case_id": str(event_data.get("id")),
        "event": str(event_data.get("event") or "api"),
        "timestamp": str(event_data.get("timestamp") or utc_now_iso()),
        "error": str(event_data.get("error") or ""),
        "payload": payload,
    }



def write_json_store(events, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)


def build_xes(events, xes_file):
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
    tree.write(xes_file, encoding="utf-8", xml_declaration=True)




def append_event(event, path, path_xes):
    try:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                events = json.load(f)
        else:
            events = []
    except (json.JSONDecodeError, OSError):
        events = []

    events.append(event)
    write_json_store(events,path)
    build_xes(events,path_xes)

    print(f"[XES] Logged event={event['event']} case_id={event['case_id']} ts={event['timestamp']}", flush=True)
    return jsonify({"status": "logged", "xes_file": str(XES_FILE)}), 200


@app.route("/log", methods=["POST"])
def log_event():
    data = request.get_json(silent=True) or {}
    if not data:
        return jsonify({"status": "invalid", "message": "No JSON payload received"}), 400


    event = normalize_event_payload(data)

    warning_event = event.get("event") in ("WindWarning", "CloudburstWarning", "SnowWarning", "WaterLevelWarning")

    if warning_event:
        append_event(event, EVENT_STORE_WARN, XES_FILE_WARN)

    append_event(event, EVENT_STORE, XES_FILE)
    return jsonify({"status": "logged"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5010)
